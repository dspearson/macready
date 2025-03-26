use std::marker::PhantomData;

use tokio::sync::mpsc;

use super::error::ProcessResult;

/// Trait for parsing a line of output into a record
pub trait StreamParser<T>: Send + Sync + 'static {
    /// Parse a line of output into a record
    fn parse(&self, line: &str) -> ProcessResult<Option<T>>;
}

/// A stream of lines from a process
pub struct LineStream {
    /// Channel receiver for lines
    rx: mpsc::Receiver<String>,
}

impl LineStream {
    /// Create a new line stream
    pub fn new(rx: mpsc::Receiver<String>) -> Self {
        Self { rx }
    }

    /// Get the next line from the stream
    pub async fn next(&mut self) -> Option<String> {
        self.rx.recv().await
    }

    /// Convert to a record stream with a parser
    pub fn parse<T, P: StreamParser<T>>(self, parser: P) -> RecordStream<T, P> {
        RecordStream::new(self.rx, parser)
    }

    /// Filter the stream
    pub fn filter<F>(self, filter: F) -> FilteredLineStream<F>
    where
        F: FnMut(&str) -> bool + Send + 'static,
    {
        FilteredLineStream::new(self.rx, filter)
    }

    /// Map the stream
    pub fn map<F, U>(self, map_fn: F) -> MappedLineStream<F, U>
    where
        F: FnMut(String) -> U + Send + 'static,
        U: Send + 'static,
    {
        MappedLineStream::new(self.rx, map_fn)
    }
}

/// A filtered stream of lines
pub struct FilteredLineStream<F>
where
    F: FnMut(&str) -> bool + Send + 'static,
{
    /// Channel receiver for lines
    rx: mpsc::Receiver<String>,

    /// Filter function
    filter: F,
}

impl<F> FilteredLineStream<F>
where
    F: FnMut(&str) -> bool + Send + 'static,
{
    /// Create a new filtered line stream
    fn new(rx: mpsc::Receiver<String>, filter: F) -> Self {
        Self { rx, filter }
    }

    /// Get the next line from the stream
    pub async fn next(&mut self) -> Option<String> {
        loop {
            match self.rx.recv().await {
                Some(line) => {
                    let filter = &mut self.filter;
                    if filter(&line) {
                        return Some(line);
                    }
                }
                None => return None,
            }
        }
    }

    /// Convert to a record stream with a parser
    pub fn parse<T, P: StreamParser<T>>(self, parser: P) -> RecordStream<T, P> {
        // Create a new channel
        let (tx, rx) = mpsc::channel(100);

        // Spawn a task to filter and forward lines
        let mut filter = self.filter;
        let mut input_rx = self.rx;

        tokio::spawn(async move {
            while let Some(line) = input_rx.recv().await {
                if filter(&line) {
                    if tx.send(line).await.is_err() {
                        break;
                    }
                }
            }
        });

        RecordStream::new(rx, parser)
    }
}

/// A mapped stream of lines
pub struct MappedLineStream<F, U>
where
    F: FnMut(String) -> U + Send + 'static,
    U: Send + 'static,
{
    /// Channel receiver for lines
    rx: mpsc::Receiver<String>,

    /// Map function
    map_fn: F,

    /// Phantom data for output type
    _output: PhantomData<U>,
}

impl<F, U> MappedLineStream<F, U>
where
    F: FnMut(String) -> U + Send + 'static,
    U: Send + 'static,
{
    /// Create a new mapped line stream
    fn new(rx: mpsc::Receiver<String>, map_fn: F) -> Self {
        Self {
            rx,
            map_fn,
            _output: PhantomData,
        }
    }

    /// Get the next mapped item from the stream
    pub async fn next(&mut self) -> Option<U> {
        self.rx.recv().await.map(|line| (self.map_fn)(line))
    }
}

/// A stream of records parsed from process output
pub struct RecordStream<T, P: StreamParser<T>> {
    /// Channel receiver for lines
    rx: mpsc::Receiver<String>,

    /// Parser for converting lines to records
    parser: P,

    /// Phantom data for record type
    _record: PhantomData<T>,
}

impl<T, P: StreamParser<T>> RecordStream<T, P> {
    /// Create a new record stream
    fn new(rx: mpsc::Receiver<String>, parser: P) -> Self {
        Self {
            rx,
            parser,
            _record: PhantomData,
        }
    }

    /// Get the next record from the stream
    pub async fn next(&mut self) -> Option<ProcessResult<T>> {
        loop {
            match self.rx.recv().await {
                Some(line) => {
                    match self.parser.parse(&line) {
                        Ok(Some(record)) => return Some(Ok(record)),
                        Ok(None) => {
                            // Parser skipped this line, continue
                            continue;
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
                None => return None,
            }
        }
    }

    /// Collect all records into a vector
    pub async fn collect(&mut self) -> ProcessResult<Vec<T>> {
        let mut records = Vec::new();

        while let Some(result) = self.next().await {
            records.push(result?);
        }

        Ok(records)
    }

    /// Filter the record stream
    pub fn filter<F>(self, filter_fn: F) -> FilteredRecordStream<T, P, F>
    where
        F: FnMut(&T) -> bool + Send + 'static,
    {
        FilteredRecordStream::new(self.rx, self.parser, filter_fn)
    }

    /// Map the record stream
    pub fn map<F, U>(self, map_fn: F) -> MappedRecordStream<T, U, P, F>
    where
        F: FnMut(T) -> U + Send + 'static,
        U: Send + 'static,
    {
        MappedRecordStream::new(self.rx, self.parser, map_fn)
    }
}

/// A filtered stream of records
pub struct FilteredRecordStream<T, P, F>
where
    P: StreamParser<T>,
    F: FnMut(&T) -> bool + Send + 'static,
{
    /// Channel receiver for lines
    rx: mpsc::Receiver<String>,

    /// Parser for converting lines to records
    parser: P,

    /// Filter function
    filter_fn: F,

    /// Phantom data for record type
    _record: PhantomData<T>,
}

impl<T, P, F> FilteredRecordStream<T, P, F>
where
    P: StreamParser<T>,
    F: FnMut(&T) -> bool + Send + 'static,
{
    /// Create a new filtered record stream
    fn new(rx: mpsc::Receiver<String>, parser: P, filter_fn: F) -> Self {
        Self {
            rx,
            parser,
            filter_fn,
            _record: PhantomData,
        }
    }

    /// Get the next record from the stream
    pub async fn next(&mut self) -> Option<ProcessResult<T>> {
        loop {
            match self.rx.recv().await {
                Some(line) => {
                    match self.parser.parse(&line) {
                        Ok(Some(record)) => {
                            if (self.filter_fn)(&record) {
                                return Some(Ok(record));
                            }
                            // Record didn't pass the filter, continue
                        }
                        Ok(None) => {
                            // Parser skipped this line, continue
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
                None => return None,
            }
        }
    }

    /// Collect all records into a vector
    pub async fn collect(&mut self) -> ProcessResult<Vec<T>> {
        let mut records = Vec::new();

        while let Some(result) = self.next().await {
            records.push(result?);
        }

        Ok(records)
    }
}

/// A mapped stream of records
pub struct MappedRecordStream<T, U, P, F>
where
    P: StreamParser<T>,
    F: FnMut(T) -> U + Send + 'static,
    U: Send + 'static,
{
    /// Channel receiver for lines
    rx: mpsc::Receiver<String>,

    /// Parser for converting lines to records
    parser: P,

    /// Map function
    map_fn: F,

    /// Phantom data for input and output types
    _input: PhantomData<T>,
    _output: PhantomData<U>,
}

impl<T, U, P, F> MappedRecordStream<T, U, P, F>
where
    P: StreamParser<T>,
    F: FnMut(T) -> U + Send + 'static,
    U: Send + 'static,
{
    /// Create a new mapped record stream
    fn new(rx: mpsc::Receiver<String>, parser: P, map_fn: F) -> Self {
        Self {
            rx,
            parser,
            map_fn,
            _input: PhantomData,
            _output: PhantomData,
        }
    }

    /// Get the next mapped item from the stream
    pub async fn next(&mut self) -> Option<ProcessResult<U>> {
        loop {
            match self.rx.recv().await {
                Some(line) => {
                    match self.parser.parse(&line) {
                        Ok(Some(record)) => {
                            return Some(Ok((self.map_fn)(record)));
                        }
                        Ok(None) => {
                            // Parser skipped this line, continue
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
                None => return None,
            }
        }
    }

    /// Collect all mapped items into a vector
    pub async fn collect(&mut self) -> ProcessResult<Vec<U>> {
        let mut items = Vec::new();

        while let Some(result) = self.next().await {
            items.push(result?);
        }

        Ok(items)
    }
}

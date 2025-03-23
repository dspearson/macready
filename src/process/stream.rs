use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use log::{debug, error, trace, warn};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;

use super::error::{ProcessError, ProcessResult};

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
                    if (self.filter)(&line) {
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
        let filter = self.filter;
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
                        },
                        Ok(None) => {
                            // Parser skipped this line, continue
                        },
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
                        },
                        Ok(None) => {
                            // Parser skipped this line, continue
                        },
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

// Common parser implementations

/// A parser for delimited text (CSV, TSV, etc.)
pub struct DelimitedTextParser<T> {
    /// Delimiter character
    delimiter: char,

    /// Whether to trim fields
    trim: bool,

    /// Whether the first line is a header
    has_header: bool,

    /// Column names (if has_header is true)
    columns: Vec<String>,

    /// Function to convert a row to a record
    converter: Arc<dyn Fn(&[String]) -> ProcessResult<T> + Send + Sync>,

    /// Whether the header has been processed
    header_processed: bool,
}

impl<T: Send + 'static> DelimitedTextParser<T> {
    /// Create a new delimited text parser
    pub fn new<F>(delimiter: char, has_header: bool, converter: F) -> Self
    where
        F: Fn(&[String]) -> ProcessResult<T> + Send + Sync + 'static,
    {
        Self {
            delimiter,
            trim: true,
            has_header,
            columns: Vec::new(),
            converter: Arc::new(converter),
            header_processed: false,
        }
    }

    /// Set whether to trim fields
    pub fn trim(mut self, trim: bool) -> Self {
        self.trim = trim;
        self
    }
}

impl<T: Send + 'static> StreamParser<T> for DelimitedTextParser<T> {
    fn parse(&self, line: &str) -> ProcessResult<Option<T>> {
        // Skip empty lines
        if line.trim().is_empty() {
            return Ok(None);
        }

        // Parse the line into fields
        let fields: Vec<String> = line
            .split(self.delimiter)
            .map(|s| if self.trim { s.trim().to_string() } else { s.to_string() })
            .collect();

        // Handle header line
        if self.has_header && !self.header_processed {
            let mut parser = self;
            // We need to cast away the const to modify the fields
            let parser = unsafe { &mut *(parser as *const Self as *mut Self) };

            parser.columns = fields;
            parser.header_processed = true;

            return Ok(None);
        }

        // Convert fields to record
        (self.converter)(&fields).map(Some)
    }
}

/// A parser for key-value pairs
pub struct KeyValueParser<T> {
    /// Delimiter between key-value pairs
    pair_delimiter: char,

    /// Delimiter between key and value
    kv_delimiter: char,

    /// Whether to trim keys and values
    trim: bool,

    /// Function to convert a map to a record
    converter: Arc<dyn Fn(&HashMap<String, String>) -> ProcessResult<T> + Send + Sync>,
}

impl<T: Send + 'static> KeyValueParser<T> {
    /// Create a new key-value parser
    pub fn new<F>(pair_delimiter: char, kv_delimiter: char, converter: F) -> Self
    where
        F: Fn(&HashMap<String, String>) -> ProcessResult<T> + Send + Sync + 'static,
    {
        Self {
            pair_delimiter,
            kv_delimiter,
            trim: true,
            converter: Arc::new(converter),
        }
    }

    /// Set whether to trim keys and values
    pub fn trim(mut self, trim: bool) -> Self {
        self.trim = trim;
        self
    }
}

impl<T: Send + 'static> StreamParser<T> for KeyValueParser<T> {
    fn parse(&self, line: &str) -> ProcessResult<Option<T>> {
        // Skip empty lines
        if line.trim().is_empty() {
            return Ok(None);
        }

        // Parse the line into key-value pairs
        let mut map = HashMap::new();

        for pair in line.split(self.pair_delimiter) {
            if let Some((key, value)) = pair.split_once(self.kv_delimiter) {
                let key = if self.trim { key.trim().to_string() } else { key.to_string() };
                let value = if self.trim { value.trim().to_string() } else { value.to_string() };

                map.insert(key, value);
            }
        }

        // Convert map to record
        (self.converter)(&map).map(Some)
    }
}

/// A parser for fixed-width columns
pub struct FixedWidthParser<T> {
    /// Column widths
    widths: Vec<usize>,

    /// Whether to trim fields
    trim: bool,

    /// Whether the first line is a header
    has_header: bool,

    /// Column names (if has_header is true)
    columns: Vec<String>,

    /// Function to convert a row to a record
    converter: Arc<dyn Fn(&[String]) -> ProcessResult<T> + Send + Sync>,

    /// Whether the header has been processed
    header_processed: bool,
}

impl<T: Send + 'static> FixedWidthParser<T> {
    /// Create a new fixed-width parser
    pub fn new<F>(widths: Vec<usize>, has_header: bool, converter: F) -> Self
    where
        F: Fn(&[String]) -> ProcessResult<T> + Send + Sync + 'static,
    {
        Self {
            widths,
            trim: true,
            has_header,
            columns: Vec::new(),
            converter: Arc::new(converter),
            header_processed: false,
        }
    }

    /// Set whether to trim fields
    pub fn trim(mut self, trim: bool) -> Self {
        self.trim = trim;
        self
    }
}

impl<T: Send + 'static> StreamParser<T> for FixedWidthParser<T> {
    fn parse(&self, line: &str) -> ProcessResult<Option<T>> {
        // Skip empty lines
        if line.trim().is_empty() {
            return Ok(None);
        }

        // Parse the line into fields based on fixed widths
        let mut fields = Vec::new();
        let mut start = 0;

        for &width in &self.widths {
            let end = (start + width).min(line.len());
            let field = if start < line.len() {
                &line[start..end]
            } else {
                ""
            };

            fields.push(if self.trim { field.trim().to_string() } else { field.to_string() });
            start += width;
        }

        // Handle header line
        if self.has_header && !self.header_processed {
            let mut parser = self;
            // We need to cast away the const to modify the fields
            let parser = unsafe { &mut *(parser as *const Self as *mut Self) };

            parser.columns = fields;
            parser.header_processed = true;

            return Ok(None);
        }

        // Convert fields to record
        (self.converter)(&fields).map(Some)
    }
}

/// A parser that handles JSON lines
pub struct JsonLinesParser<T> {
    /// Phantom data for record type
    _record: PhantomData<T>,
}

impl<T> JsonLinesParser<T>
where
    T: for<'de> serde::Deserialize<'de> + Send + 'static,
{
    /// Create a new JSON lines parser
    pub fn new() -> Self {
        Self {
            _record: PhantomData,
        }
    }
}

impl<T> StreamParser<T> for JsonLinesParser<T>
where
    T: for<'de> serde::Deserialize<'de> + Send + 'static,
{
    fn parse(&self, line: &str) -> ProcessResult<Option<T>> {
        // Skip empty lines
        if line.trim().is_empty() {
            return Ok(None);
        }

        // Parse JSON
        serde_json::from_str(line)
            .map(Some)
            .map_err(|e| ProcessError::ParseError(format!("Invalid JSON: {}", e)))
    }
}

/// A parser that calls a custom function for each line
pub struct CustomParser<T, F>
where
    F: Fn(&str) -> ProcessResult<Option<T>> + Send + Sync,
{
    /// Function to parse a line
    parse_fn: F,

    /// Phantom data for record type
    _record: PhantomData<T>,
}

impl<T, F> CustomParser<T, F>
where
    F: Fn(&str) -> ProcessResult<Option<T>> + Send + Sync,
{
    /// Create a new custom parser
    pub fn new(parse_fn: F) -> Self {
        Self {
            parse_fn,
            _record: PhantomData,
        }
    }
}

impl<T, F> StreamParser<T> for CustomParser<T, F>
where
    F: Fn(&str) -> ProcessResult<Option<T>> + Send + Sync,
{
    fn parse(&self, line: &str) -> ProcessResult<Option<T>> {
        (self.parse_fn)(line)
    }
}

/// Trait for creating stream parsers
pub trait StreamParserBuilder<T> {
    /// Build a stream parser
    fn build(&self) -> Box<dyn StreamParser<T>>;
}

/// Builder for delimited text parsers
pub struct DelimitedTextParserBuilder<T, F>
where
    F: Fn(&[String]) -> ProcessResult<T> + Send + Sync + 'static,
{
    /// Delimiter character
    delimiter: char,

    /// Whether to trim fields
    trim: bool,

    /// Whether the first line is a header
    has_header: bool,

    /// Function to convert a row to a record
    converter: F,

    /// Phantom data for record type
    _record: PhantomData<T>,
}

impl<T, F> DelimitedTextParserBuilder<T, F>
where
    T: Send + 'static,
    F: Fn(&[String]) -> ProcessResult<T> + Send + Sync + 'static,
{
    /// Create a new delimited text parser builder
    pub fn new(delimiter: char, converter: F) -> Self {
        Self {
            delimiter,
            trim: true,
            has_header: false,
            converter,
            _record: PhantomData,
        }
    }

    /// Set whether to trim fields
    pub fn trim(mut self, trim: bool) -> Self {
        self.trim = trim;
        self
    }

    /// Set whether the first line is a header
    pub fn has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }
}

impl<T, F> StreamParserBuilder<T> for DelimitedTextParserBuilder<T, F>
where
    T: Send + 'static,
    F: Fn(&[String]) -> ProcessResult<T> + Send + Sync + 'static,
{
    fn build(&self) -> Box<dyn StreamParser<T>> {
        Box::new(DelimitedTextParser::new(self.delimiter, self.has_header, self.converter.clone())
            .trim(self.trim))
    }
}

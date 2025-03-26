use std::collections::HashMap;
use std::marker::PhantomData;

use crate::process::error::{ProcessError, ProcessResult};
use crate::process::stream::StreamParser;

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
    converter: Box<dyn Fn(&[String]) -> ProcessResult<T> + Send + Sync>,

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
            converter: Box::new(converter),
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
            .map(|s| {
                if self.trim {
                    s.trim().to_string()
                } else {
                    s.to_string()
                }
            })
            .collect();

        // Handle header line
        if self.has_header && !self.header_processed {
            let parser = self as *const Self as *mut Self;
            // We need to cast away the const to modify the fields
            unsafe {
                (*parser).columns = fields;
                (*parser).header_processed = true;
            }

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
    converter: Box<dyn Fn(&HashMap<String, String>) -> ProcessResult<T> + Send + Sync>,
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
            converter: Box::new(converter),
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
                let key = if self.trim {
                    key.trim().to_string()
                } else {
                    key.to_string()
                };
                let value = if self.trim {
                    value.trim().to_string()
                } else {
                    value.to_string()
                };

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
    converter: Box<dyn Fn(&[String]) -> ProcessResult<T> + Send + Sync>,

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
            converter: Box::new(converter),
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

            fields.push(if self.trim {
                field.trim().to_string()
            } else {
                field.to_string()
            });
            start += width;
        }

        // Handle header line
        if self.has_header && !self.header_processed {
            let parser = self as *const Self as *mut Self;
            // We need to cast away the const to modify the fields
            unsafe {
                (*parser).columns = fields;
                (*parser).header_processed = true;
            }

            return Ok(None);
        }

        // Convert fields to record
        (self.converter)(&fields).map(Some)
    }
}

/// A parser that handles JSON lines
pub struct JsonLinesParser<T>
where
    T: for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
{
    /// Phantom data for record type
    _record: PhantomData<T>,
}

impl<T> JsonLinesParser<T>
where
    T: for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
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
    T: for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
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
    T: Send + Sync + 'static,
    F: Fn(&str) -> ProcessResult<Option<T>> + Send + Sync,
{
    /// Function to parse a line
    parse_fn: F,

    /// Phantom data for record type
    _record: PhantomData<T>,
}

impl<T, F> CustomParser<T, F>
where
    T: Send + Sync + 'static,
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
    T: Send + Sync + 'static,
    F: Fn(&str) -> ProcessResult<Option<T>> + Send + Sync + 'static,
{
    fn parse(&self, line: &str) -> ProcessResult<Option<T>> {
        (self.parse_fn)(line)
    }
}

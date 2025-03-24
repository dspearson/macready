use crate::error::Result;
use std::fmt::Debug;
use std::marker::PhantomData;

/// Trait for transforming raw metrics into standardized metrics
#[async_trait::async_trait]
pub trait MetricTransformer<I, O>: Send + Sync + 'static {
    /// Transform raw input metrics into standardized output metrics
    async fn transform(&self, input: I) -> Result<Vec<O>>;

    /// Get the name of this transformer
    fn name(&self) -> &str;

    /// Get the source of the metrics
    fn source(&self) -> &str;
}

/// A simple transformer that applies a function to each input
pub struct FunctionTransformer<I, O, F>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: Fn(I) -> Result<Vec<O>> + Send + Sync + 'static,
{
    /// The transformation function
    transform_fn: F,
    /// The name of this transformer
    name: String,
    /// The source of the metrics
    source: String,
    /// Phantom data for input and output types
    _phantom: PhantomData<(I, O)>,
}

impl<I, O, F> FunctionTransformer<I, O, F>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: Fn(I) -> Result<Vec<O>> + Send + Sync + 'static,
{
    /// Create a new function transformer
    pub fn new(name: impl Into<String>, source: impl Into<String>, transform_fn: F) -> Self {
        Self {
            transform_fn,
            name: name.into(),
            source: source.into(),
            _phantom: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<I, O, F> MetricTransformer<I, O> for FunctionTransformer<I, O, F>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: Fn(I) -> Result<Vec<O>> + Send + Sync + 'static,
{
    async fn transform(&self, input: I) -> Result<Vec<O>> {
        (self.transform_fn)(input)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn source(&self) -> &str {
        &self.source
    }
}

/// A transformer that chains multiple transformers together
pub struct ChainedTransformer<I, M, O> {
    /// The first transformer
    first: Box<dyn MetricTransformer<I, M>>,
    /// The second transformer
    second: Box<dyn MetricTransformer<M, O>>,
    /// The name of this transformer
    name: String,
}

impl<I, M, O> ChainedTransformer<I, M, O> {
    /// Create a new chained transformer
    pub fn new(
        name: impl Into<String>,
        first: impl MetricTransformer<I, M> + 'static,
        second: impl MetricTransformer<M, O> + 'static,
    ) -> Self {
        Self {
            first: Box::new(first),
            second: Box::new(second),
            name: name.into(),
        }
    }
}

#[async_trait::async_trait]
impl<I, M, O> MetricTransformer<I, O> for ChainedTransformer<I, M, O>
where
    I: Send + Sync + 'static,
    M: Send + Sync + Debug + 'static,
    O: Send + Sync + 'static,
{
    async fn transform(&self, input: I) -> Result<Vec<O>> {
        // Apply the first transformer
        let intermediates = self.first.transform(input).await?;

        // Apply the second transformer to each intermediate result
        let mut results = Vec::new();
        for intermediate in intermediates {
            let mut output = self.second.transform(intermediate).await?;
            results.append(&mut output);
        }

        Ok(results)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn source(&self) -> &str {
        self.first.source()
    }
}

/// A transformer that filters metrics
pub struct FilteredTransformer<I, O, F>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: Fn(&I) -> bool + Send + Sync + 'static,
{
    /// The inner transformer
    inner: Box<dyn MetricTransformer<I, O>>,
    /// The filter function
    filter: F,
    /// The name of this transformer
    name: String,
}

impl<I, O, F> FilteredTransformer<I, O, F>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: Fn(&I) -> bool + Send + Sync + 'static,
{
    /// Create a new filtered transformer
    pub fn new(
        name: impl Into<String>,
        inner: impl MetricTransformer<I, O> + 'static,
        filter: F,
    ) -> Self {
        Self {
            inner: Box::new(inner),
            filter,
            name: name.into(),
        }
    }
}

#[async_trait::async_trait]
impl<I, O, F> MetricTransformer<I, O> for FilteredTransformer<I, O, F>
where
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: Fn(&I) -> bool + Send + Sync + 'static,
{
    async fn transform(&self, input: I) -> Result<Vec<O>> {
        // Apply the filter
        if !(self.filter)(&input) {
            return Ok(Vec::new());
        }

        // Apply the inner transformer
        self.inner.transform(input).await
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn source(&self) -> &str {
        self.inner.source()
    }
}

/// A transformer builder
pub struct TransformerBuilder;

impl TransformerBuilder {
    /// Create a new function transformer
    pub fn function<I, O, F>(
        name: impl Into<String>,
        source: impl Into<String>,
        transform_fn: F,
    ) -> FunctionTransformer<I, O, F>
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
        F: Fn(I) -> Result<Vec<O>> + Send + Sync + 'static,
    {
        FunctionTransformer::new(name, source, transform_fn)
    }

    /// Create a new chained transformer
    pub fn chain<I, M, O>(
        name: impl Into<String>,
        first: impl MetricTransformer<I, M> + 'static,
        second: impl MetricTransformer<M, O> + 'static,
    ) -> ChainedTransformer<I, M, O> {
        ChainedTransformer::new(name, first, second)
    }

    /// Create a new filtered transformer
    pub fn filter<I, O, F>(
        name: impl Into<String>,
        inner: impl MetricTransformer<I, O> + 'static,
        filter: F,
    ) -> FilteredTransformer<I, O, F>
    where
        I: Send + Sync + 'static,
        O: Send + Sync + 'static,
        F: Fn(&I) -> bool + Send + Sync + 'static,
    {
        FilteredTransformer::new(name, inner, filter)
    }
}

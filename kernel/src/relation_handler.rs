use std::sync::{Arc, LazyLock};

use crate::{
    AsAny, EngineData, EngineRelationRef, InMemoryEngineRelation, KernelError, Result,
    ResultIteratorStatic,
};

/// Reads connector-managed immutable relations.
pub trait RelationHandler: AsAny {
    /// Reads a connector-defined relation.
    fn read_relation_impl(
        &self,
        relation: EngineRelationRef,
    ) -> Result<ResultIteratorStatic<Arc<dyn EngineData>>>;

    /// Reads an immutable relation.
    fn read_relation(
        &self,
        relation: EngineRelationRef,
    ) -> Result<ResultIteratorStatic<Arc<dyn EngineData>>> {
        let in_memory: std::result::Result<Arc<InMemoryEngineRelation>, _> =
            Arc::clone(&relation).as_any().downcast();
        match in_memory {
            Ok(relation) => Ok(Box::new(relation.into_iter().map(Ok))),
            Err(_) => self.read_relation_impl(relation),
        }
    }
}

struct InMemoryRelationHandler;

impl RelationHandler for InMemoryRelationHandler {
    fn read_relation_impl(
        &self,
        _relation: EngineRelationRef,
    ) -> Result<ResultIteratorStatic<Arc<dyn EngineData>>> {
        Err(KernelError::unsupported(
            "this handler cannot read the relation",
        ))
    }
}

pub(crate) fn default_relation_handler() -> Arc<dyn RelationHandler> {
    static HANDLER: LazyLock<Arc<dyn RelationHandler>> =
        LazyLock::new(|| Arc::new(InMemoryRelationHandler));
    Arc::clone(&HANDLER)
}

pub mod continuous;
pub mod oneshot;
pub mod periodic;

use super::registry::TaskRegistry;

impl TaskRegistry {
    pub fn periodic(&self, name: &'static str) -> periodic::PeriodicBuilder<'_, ()> {
        periodic::PeriodicBuilder::new(self, name)
    }

    pub fn continuous(&self, name: &'static str) -> continuous::ContinuousBuilder<'_, ()> {
        continuous::ContinuousBuilder::new(self, name)
    }

    pub fn oneshot(&self, name: &'static str) -> oneshot::OneShotBuilder<'_, ()> {
        oneshot::OneShotBuilder::new(self, name)
    }
}

use std::ops::{Deref, DerefMut};

pub trait IntoComponents {
    type Components;
    fn into_components(self) -> Self::Components;
}

pub struct Borrow;
pub struct RefCell;

mod private {
    pub trait Sealed {}
}

//TODO: Maybe two seperate traits for Ref and RefMut.
pub trait ComponentsAsRefs<T, C>: private::Sealed {
    type RefMapping<'a>;
    type RefMutMapping<'a>;
}

macro_rules! gen_ref_for {
    ($field:ident) => {
        &'a C
    };
}
macro_rules! gen_ref_for_mut {
    ($field:ident) => {
        &'a mut C
    };
}
macro_rules! gen_cell_ref_for {
    ($field:ident) => {
        std::cell::Ref<'a, C>
    };
}
macro_rules! gen_cell_ref_for_mut {
    ($field:ident) => {
        std::cell::RefMut<'a, C>
    };
}
macro_rules! impl_components_for_slab_as_refs {
    ($T:ident) => {
        impl<$T> private::Sealed for ($T,) {}

        impl<$T, C> ComponentsAsRefs<Borrow, C> for ($T,)
        where
            for<'a> C: IntoIterator<Item = (usize, ($T,))> + 'a,
            for<'a> $T: 'a,
        {
            type RefMapping<'a> = &'a C;
            type RefMutMapping<'a> = &'a mut C;
        }

        impl<$T, C> ComponentsAsRefs<RefCell, C> for ($T,)
        where
            for<'a> C: IntoIterator<Item = (usize, ($T,))> + 'a,
            for<'a> $T: 'a,
        {
            type RefMapping<'a> = std::cell::Ref<'a, C>;
            type RefMutMapping<'a> = std::cell::RefMut<'a, C>;
        }
    };

    ($T:ident, $($rest:ident),+) => {
        impl<$T, $($rest),+> private::Sealed for ($T, $($rest),+) {}

        impl<$T, $($rest),+, C> ComponentsAsRefs<Borrow, C> for ($T, $($rest),+)
        where
            for<'a> C: IntoIterator<Item = (usize, ($T,))> + 'a,
            //$(for<'a> C: IntoIterator<Item = (usize, ($rest,))> + 'a),+,
            for<'a> $T: 'a,
            $(for<'a> $rest: 'a),+,
        {
            type RefMapping<'a> = (&'a C, $(gen_ref_for!($rest)),+);
            type RefMutMapping<'a> = (&'a mut C, $(gen_ref_for_mut!($rest)),+);
        }

        impl<$T, $($rest),+, C> ComponentsAsRefs<RefCell, C> for ($T, $($rest),+)
        where
            for<'a> C: IntoIterator<Item = (usize, ($T, $($rest),+))> + 'a,
            for<'a> $T: 'a,
            $(for<'a> $rest: 'a),+,
        {
            type RefMapping<'a> = (std::cell::Ref<'a, C>, $(gen_cell_ref_for!($rest)),+);
            type RefMutMapping<'a> = (std::cell::RefMut<'a, C>, $(gen_cell_ref_for_mut!($rest)),+);
        }

        impl_components_for_slab_as_refs!($($rest),+);
    };
}
impl_components_for_slab_as_refs!(T1, T2, T3, T4, T5, T6, T7, T8);

type Mapping<'a, E, T, C> =
    <<E as IntoComponents>::Components as ComponentsAsRefs<T, C>>::RefMapping<'a>;
type MappingMut<'a, E, T, C> =
    <<E as IntoComponents>::Components as ComponentsAsRefs<T, C>>::RefMutMapping<'a>;

pub type Components<T> = <T as IntoComponents>::Components;

pub trait EntityComponentSystem<T, C>
where
    C: IntoIterator,
    <Self::Entity as IntoComponents>::Components: ComponentsAsRefs<T, C>,
{
    type Entity: IntoComponents;
    type EntityRef<'a>: IntoComponents<Components = Mapping<'a, Self::Entity, T, C>>
    where
        Self: 'a;
    fn with<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Components<Self::EntityRef<'a>>) -> O;

    fn with_async<O, F>(&self, f: F) -> impl Future<Output = O>
    where
        F: for<'a> FnOnce(Components<Self::EntityRef<'a>>) -> O;
}

pub trait EntityComponentSystemMut<C>: EntityComponentSystem<Borrow, C>
where
    C: IntoIterator,
{
    type EntityRefMut<'a>: IntoComponents<Components = MappingMut<'a, Self::Entity, Borrow, C>>
    where
        Self: 'a;

    fn with_mut<O, F>(&mut self, f: F) -> O
    where
        F: for<'a> FnOnce(Components<Self::EntityRefMut<'a>>) -> O;
}

pub trait EntityComponentSystemMutCell<C>: EntityComponentSystem<RefCell, C>
where
    C: IntoIterator,
{
    type EntityRefMut<'a>: IntoComponents<Components = MappingMut<'a, Self::Entity, RefCell, C>>
    where
        Self: 'a;

    fn with_mut<O, F>(&mut self, f: F) -> O
    where
        F: for<'a> FnOnce(Components<Self::EntityRefMut<'a>>) -> O;
}

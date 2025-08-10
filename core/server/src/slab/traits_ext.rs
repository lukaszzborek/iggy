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
pub trait ComponentsMapping<T>: private::Sealed {
    type Ref<'a>;
    type RefMut<'a>;
}

macro_rules! impl_components_for_slab_as_refs {
    ($T:ident) => {
        impl<$T> private::Sealed for ($T,) {}

        impl<$T> ComponentsMapping<Borrow> for ($T,)
        where for<'a> $T: 'a
        {
            type Ref<'a> = (&'a ::slab::Slab<$T>,);
            type RefMut<'a> = (&'a mut ::slab::Slab<$T>,);
        }

        impl<$T> ComponentsMapping<RefCell> for ($T,)
        where for<'a> $T: 'a
        {
            type Ref<'a> = (::std::cell::Ref<'a, ::slab::Slab<$T>>,);
            type RefMut<'a> = (::std::cell::RefMut<'a, ::slab::Slab<$T>>,);
        }
    };

    ($T:ident, $($rest:ident),+) => {
        impl<$T, $($rest),+> private::Sealed for ($T, $($rest),+) {}

        impl<$T, $($rest),+> ComponentsMapping<Borrow> for ($T, $($rest),+)
        where
            for<'a> $T: 'a,
            $(for<'a> $rest: 'a),+
        {
            type Ref<'a> = (&'a ::slab::Slab<$T>, $(&'a ::slab::Slab<$rest>),+);
            type RefMut<'a> = (&'a mut ::slab::Slab<$T>, $(&'a mut ::slab::Slab<$rest>),+);
        }

        impl<$T, $($rest),+> ComponentsMapping<RefCell> for ($T, $($rest),+)
        where
            for<'a> $T: 'a,
            $(for<'a> $rest: 'a),+
        {
            type Ref<'a> = (std::cell::Ref<'a, ::slab::Slab<$T>>, $(::std::cell::Ref<'a, ::slab::Slab<$rest>>),+);
            type RefMut<'a> = (std::cell::RefMut<'a, ::slab::Slab<$T>>, $(::std::cell::RefMut<'a, ::slab::Slab<$rest>>),+);
        }
        impl_components_for_slab_as_refs!($($rest),+);
    };
}
impl_components_for_slab_as_refs!(T1, T2, T3, T4, T5, T6, T7, T8);

type Mapping<'a, E, T> = <<E as IntoComponents>::Components as ComponentsMapping<T>>::Ref<'a>;
type MappingMut<'a, E, T> = <<E as IntoComponents>::Components as ComponentsMapping<T>>::RefMut<'a>;

pub type Components<T> = <T as IntoComponents>::Components;

pub trait EntityComponentSystem<T>
where
    <Self::Entity as IntoComponents>::Components: ComponentsMapping<T>,
{
    type Entity: IntoComponents;
    type EntityRef<'a>: IntoComponents<Components = Mapping<'a, Self::Entity, T>>
    where
        Self: 'a;
    fn with<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Components<Self::EntityRef<'a>>) -> O;

    fn with_async<O, F>(&self, f: F) -> impl Future<Output = O>
    where
        F: for<'a> FnOnce(Components<Self::EntityRef<'a>>) -> O;
}

pub trait EntityComponentSystemMut: EntityComponentSystem<Borrow> {
    type EntityRefMut<'a>: IntoComponents<Components = MappingMut<'a, Self::Entity, Borrow>>
    where
        Self: 'a;

    fn with_mut<O, F>(&mut self, f: F) -> O
    where
        F: for<'a> FnOnce(Components<Self::EntityRefMut<'a>>) -> O;
}

pub trait EntityComponentSystemMutCell: EntityComponentSystem<RefCell> {
    type EntityRefMut<'a>: IntoComponents<Components = MappingMut<'a, Self::Entity, RefCell>>
    where
        Self: 'a;

    fn with_mut<O, F>(&mut self, f: F) -> O
    where
        F: for<'a> FnOnce(Components<Self::EntityRefMut<'a>>) -> O;
}

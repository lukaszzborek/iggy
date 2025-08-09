pub trait IntoComponents {
    type Components;
    fn into_components(self) -> Self::Components;
}

pub struct Borrow;
pub struct RefCell;

mod private {
    pub trait Sealed {}
}

pub trait ComponentsAsRefs<RefType>: private::Sealed {
    type RefMapping<'a>;
    type RefMutMapping<'a>;
}
macro_rules! impl_components_as_refs {
    ($T:ident) => {
        impl<$T> private::Sealed for ($T,) {}
        
        impl<$T> ComponentsAsRefs<Borrow> for ($T,)
        where for<'a> $T: 'a
        {
            type RefMapping<'a> = (&'a $T,);
            type RefMutMapping<'a> = (&'a mut $T,);
        }

        impl<$T> ComponentsAsRefs<RefCell> for ($T,)
        where for<'a> $T: 'a
        {
            type RefMapping<'a> = (std::cell::Ref<'a, $T>,);
            type RefMutMapping<'a> = (std::cell::RefMut<'a, $T>,);
        }
    };

    ($T:ident, $($rest:ident),+) => {
        impl<$T, $($rest),+> private::Sealed for ($T, $($rest),+) {}
        
        impl<$T, $($rest),+> ComponentsAsRefs<Borrow> for ($T, $($rest),+)
        where
            for<'a> $T: 'a,
            $(for<'a> $rest: 'a),+
        {
            type RefMapping<'a> = (&'a $T, $(&'a $rest),+);
            type RefMutMapping<'a> = (&'a mut $T, $(&'a mut $rest),+);
        }

        impl<$T, $($rest),+> ComponentsAsRefs<RefCell> for ($T, $($rest),+)
        where
            for<'a> $T: 'a,
            $(for<'a> $rest: 'a),+
        {
            type RefMapping<'a> = (std::cell::Ref<'a, $T>, $(std::cell::Ref<'a, $rest>),+);
            type RefMutMapping<'a> = (std::cell::RefMut<'a, $T>, $(std::cell::RefMut<'a, $rest>),+);
        }
        impl_components_as_refs!($($rest),+);
    };
}
impl_components_as_refs!(T1, T2, T3, T4, T5, T6, T7, T8);

type ComponentsTuple<'a, T, RefType> =
    <<T as IntoComponents>::Components as ComponentsAsRefs<RefType>>::RefMapping<'a>;
type ComponentsTupleMut<'a, T, RefType> =
    <<T as IntoComponents>::Components as ComponentsAsRefs<RefType>>::RefMutMapping<'a>;

// TODO: extend it with a method `with_by_id`, this one actually will use the `EntityRef`
// those `with` methods need to work on `impl Iterator<Item = ComponentsAsRefs individual components>`
pub trait EntityComponentSystem<RefType> {
    type Entity: IntoComponents;
    type EntityRef<'a>: IntoComponents<Components = ComponentsTuple<'a, Self::Entity, RefType>>
    where
        <Self::Entity as IntoComponents>::Components: ComponentsAsRefs<RefType>,
        Self: 'a;

    fn with_new<T, F>(&self, f: F) -> T
    where
        F: FnOnce(Self::EntityRef<'_>) -> T;

    fn with_async_new<T, F>(&self, f: F) -> impl Future<Output = T>
    where
        F: FnOnce(Self::EntityRef<'_>) -> T;
}

pub trait EntityComponentSystemMut: EntityComponentSystem<Borrow> {
    type EntityRefMut<'a>: IntoComponents<Components = ComponentsTupleMut<'a, Self::Entity, Borrow>>
    where
        <Self::Entity as IntoComponents>::Components: ComponentsAsRefs<Borrow>,
        Self: 'a;

    fn with_mut_new<T, F>(&mut self, f: F) -> T
    where
        F: FnOnce(Self::EntityRefMut<'_>) -> T;
}

pub trait EntityComponentSystemMutCell: EntityComponentSystem<RefCell> {
    type EntityRefMut<'a>: IntoComponents<
        Components = ComponentsTupleMut<'a, Self::Entity, RefCell>,
    >
    where
        <Self::Entity as IntoComponents>::Components: ComponentsAsRefs<RefCell>,
        Self: 'a;

    fn with_mut_new<T, F>(&mut self, f: F) -> T
    where
        F: FnOnce(Self::EntityRefMut<'_>) -> T;
}

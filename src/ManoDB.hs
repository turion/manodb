{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE DeriveFoldable #-}
module ManoDB where

-- base
import Data.Foldable
import Data.Functor.Identity
import Data.Maybe (fromJust)

-- transformers
import Control.Monad.Trans.State.Strict

-- fingertree
import Data.FingerTree hiding (empty)
import qualified Data.FingerTree as FingerTree

class Index i v | v -> i where
  index :: v -> i

newtype PrimaryKey = PrimaryKey
  { getPrimaryKey :: Integer
  }
  deriving (Eq, Ord, Num, Show)

data IndexRange index = NoRange
  | IndexRange
    { low :: index
    , high :: index
    }
  deriving Eq

point :: index -> IndexRange index
point index = IndexRange index index

instance Ord index => Semigroup (IndexRange index) where
  NoRange <> range = range
  range <> NoRange = range
  range1 <> range2 = IndexRange
    { low = min (low range1) (low range2)
    , high = max (high range1) (high range2)
    }
instance Ord index => Monoid (IndexRange index) where
  mempty = NoRange

data IndexedBy i v = IndexedBy
  { indexedValue :: v
  , cachedIndex :: i
  }
  deriving (Show, Foldable)

cacheIndex :: Index i v => v -> IndexedBy i v
cacheIndex indexedValue = IndexedBy
  { cachedIndex = index indexedValue
  , indexedValue
  }

instance Ord i => Measured (IndexRange i) (IndexedBy i v) where
  measure = point . cachedIndex

-- TODO make an Indexed struct
type MainTable index v = FingerTree (IndexRange PrimaryKey) (IndexedBy PrimaryKey v)
type IndexTable index = FingerTree (IndexRange index) (IndexedBy index PrimaryKey)

-- TODO allow more than 1 index
data ManoDB index v = ManoDB
  { mainTable :: MainTable index v
  , indexTable :: IndexTable index
  , maxPrimaryKey :: PrimaryKey
  }
  deriving (Show, Foldable)

empty :: Ord index => ManoDB index v
empty = ManoDB
 { mainTable = FingerTree.empty
 , indexTable = FingerTree.empty
 , maxPrimaryKey = 0
 }

newtype ManoT index v m a = ManoT
  { unManoT :: StateT (ManoDB index v) m a
  }
  deriving (Functor, Applicative, Monad)

runManoT :: (Monad m, Ord index) => ManoT index v m a -> m a
runManoT = flip evalStateT empty . unManoT

type Mano index v a = ManoT index v Identity a

runMano :: Ord index => Mano index v a -> a
runMano = runIdentity . runManoT

runMyMano :: Mano String String a -> a
runMyMano = runMano

dumpMyMano :: Mano String String a -> ManoDB String String
dumpMyMano = runIdentity . flip execStateT empty . unManoT

instance Index String String where
  index = id

insertIndex :: Ord index => index -> PrimaryKey -> IndexTable index -> IndexTable index
insertIndex i primaryKey indexTable =
  let isNotLower IndexRange { low } = i >= low
      (lower, higher) = split isNotLower indexTable
      entry = IndexedBy { indexedValue = primaryKey, cachedIndex = i }
  in (lower |> entry) >< higher

insert :: (Monad m, Index index v, Ord index) => v -> ManoT index v m (PrimaryKey, index)
insert v = ManoT $ do
  let i = index v
  ManoDB { mainTable, indexTable, maxPrimaryKey } <- get
  let primaryKey = maxPrimaryKey + 1
  put $ ManoDB
    { mainTable = mainTable |> IndexedBy { cachedIndex = primaryKey, indexedValue = v }
    , indexTable = insertIndex i primaryKey indexTable
    , maxPrimaryKey = primaryKey
    }
  return (primaryKey, i)

all :: Monad m => ManoT index v m [v]
all = ManoT $ toList <$> get

lookupByPrimaryKey :: MainTable index v -> PrimaryKey -> Maybe v
lookupByPrimaryKey mainTable primaryKey =
  let
    (lower, contains) = split ((primaryKey <=) . low) mainTable
  in case viewl contains of
    EmptyL -> Nothing
    IndexedBy { indexedValue } :< _ -> Just indexedValue

getByPrimaryKey :: Monad m => PrimaryKey -> ManoT index v m (Maybe v)
getByPrimaryKey primaryKey = ManoT $ do
  ManoDB { mainTable } <- get
  return $ lookupByPrimaryKey mainTable primaryKey

ordered :: Monad m => ManoT index v m [v]
ordered = ManoT $ do
  ManoDB
    { mainTable
    , indexTable
    } <- get
  let
    primaryKeys = indexedValue <$> toList indexTable
  return $ fromJust . lookupByPrimaryKey mainTable <$> primaryKeys


-- FIXME datatype of performant queries that use a particular multiindex
{-
data Query multiindex where
  -- | Gets everything
  All :: Query multiindex
  -- | After an 'Equals' query, we can have further queries
  Equals :: Eq a => a -> Query indices -> Query (Index a) indices
  -- | A 'Range' query must be the last one
  Range ::
    Ord a =>
    a ->
    Direction ->
    Boundary ->
    -- | Possibly bound in the opposite direction
    Maybe (a, Boundary) ->
    Query (Index a) indices

data Direction = Upper | Lower
data Boundary = Inclusive | Exclusive
-}

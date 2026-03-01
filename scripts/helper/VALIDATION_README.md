# Category-to-ID and Sparse Matrix Validation

## Purpose

Cross-validate that category strings from **Meta Data** → **Category_Lookup** (string → `unique_category_id`) → **Enriched Lookup Table** (`category_ids` per product) → **SparseMatrix** (row = product, column = category ID) stay consistent for Magazine_Subscriptions.

## How to Run

In MATLAB:

```matlab
cd('c:\Users\sheba\Desktop\Amazon Data\scripts\helper')
Validate_Category_Mapping_Report
```

You will be prompted to select these four files (in order):

1. **meta_Magazine_Subscriptions_part1.mat** – raw meta (parent_asin, categories)
2. **meta_Magazine_Subscriptions_Category_Lookup.mat** – string → unique_category_id table
3. **Magazine_SubscriptionsLookUpTable_Enriched.mat** – products with parent_asin and category_ids (defines matrix row order)
4. **Magazine_SubscriptionsLookUpTable_Enriched_SparseMatrix.mat** (or your saved SparseMatrix .mat)

The script prints a **Validation Report** and checks 5 random products across the three sources and the matrix.

## Code Audit Summary (Indexing)

- **1-based indexing everywhere:** MATLAB uses 1-based indices; `unique_category_id` is assigned as `categoryMap.Count + 1` in `Amazon_meta_data_script.m`, so the first category has ID 1.
- **Matrix rows:** Row `i` in the SparseMatrix = row `i` in the **Enriched Lookup Table** (same order). So row index = table row index, **not** `unique_id` value. The Enriched table row order is that of the original lookup table (e.g. from Restore_LookupTable / saved lookup).
- **Matrix columns:** Column `j` = category with `unique_category_id == j`. So `SparseMatrix(i,j) == 1` means product (table row `i`) belongs to category `j`.
- **Where logic can fail:**
  - **Row mismatch:** If the SparseMatrix was built from a different Enriched table (e.g. different row order or different number of rows), matrix row `i` would not correspond to the same product as Enriched table row `i`. The script checks `size(sparseMatrix,1) == height(enrTable)`.
  - **Column mismatch:** If Category_Lookup used when building the Enriched table differs from the one used in `generate_category_sparse_matrix.m`, or if `numCategories = max(catTable.unique_category_id)` is wrong (e.g. gaps in IDs are fine, but a different table could change column meaning). The script checks column count vs `max(unique_category_id)`.
  - **Empty category_ids:** In `generate_category_sparse_matrix.m`, `J = [mainTable.category_ids{:}]'` and `I = repelem(1:numProducts, counts)'`; empty cells are handled (counts=0, no entries for that row). No bug there.
  - **parent_asin not in Meta:** If a product’s parent_asin never appears in the meta part file(s) that were used to build the map in Enrich_Lookup_Table, that product’s `category_ids` will stay empty and the matrix row will have no 1s. The validation only samples products that appear in both Enriched and Meta.

## Files Produced by Pipeline

| File | Produced by | Content |
|------|-------------|---------|
| meta_*_part1.mat | Amazon_meta_data_script.m | partialDataStruct: parent_asin, categories (strings), ... |
| meta_*_Category_Lookup.mat | Amazon_meta_data_script.m | categoryLookupTable: unique_category_id, category |
| *LookUpTable_Enriched.mat | Enrich_Lookup_Table.m | lookupTableStruct: unique_id, asin, parent_asin, category_ids (numeric vector) |
| *_SparseMatrix.mat | generate_category_sparse_matrix.m | sparseMatrix (products × categories), numProducts, numCategories |

## If Validation Fails

- **"Meta vs Enriched IDs match: NO"** – Category_Lookup or the Parent→Categories map in Enrich_Lookup_Table might use a different meta set or different string normalization.
- **"SparseMatrix(row,...) all 1: NO"** – Either the SparseMatrix was built from another Enriched table (row order/number), or the Enriched table was modified after building the matrix. Ensure the SparseMatrix was generated from the **same** Enriched table file you pass to the validation script.

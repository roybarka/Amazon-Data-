function ok = generate_category_sparse_matrix(cfg)
% GENERATE_CATEGORY_SPARSE_MATRIX  Build Products x Categories sparse matrix from Enriched + Category lookup.
% Fully automated: no GUI. Uses config with Category -> File Type paths.
%
% cfg.categoryRoot, cfg.categoryName.
% Reads: LookUp_Table/<Category>LookUpTable_Enriched.mat, Meta_Data_LookUp_Table/meta_<Category>_Category_Lookup.mat.
% Writes: Asin_Category_Matrix/<Category>LookUpTable_Enriched_SparseMatrix.mat.

ok = false;
if nargin < 1 || ~isstruct(cfg)
    error('generate_category_sparse_matrix requires config struct cfg with categoryRoot, categoryName.');
end

paths = pipeline_getCategoryPaths(cfg.categoryRoot, cfg.categoryName);
enrichedPath = paths.lookupTableEnrichedFile;
catLookupPath = paths.metaCategoryLookupFile;
outputPath = paths.sparseMatrixFile;

if ~exist(enrichedPath, 'file')
    error('Enriched lookup not found: %s', enrichedPath);
end
if ~exist(catLookupPath, 'file')
    error('Category lookup not found: %s', catLookupPath);
end

asinCatDir = fileparts(outputPath);
if ~exist(asinCatDir, 'dir')
    mkdir(asinCatDir);
end

% Load Enriched Lookup Table
loadedData = load(enrichedPath);
if isfield(loadedData, 'lookupTableStruct')
    mainTable = struct2table(loadedData.lookupTableStruct);
elseif isfield(loadedData, 'lookupTable')
    mainTable = loadedData.lookupTable;
elseif isfield(loadedData, 'mainTable')
    mainTable = loadedData.mainTable;
else
    vars = fieldnames(loadedData);
    mainTable = loadedData.(vars{1});
    if isstruct(mainTable), mainTable = struct2table(mainTable); end
end

if ~ismember('category_ids', mainTable.Properties.VariableNames)
    error('Enriched table does not have "category_ids" column. Run Enrich_Lookup_Table first.');
end

% Load Category Lookup Table
loadedCat = load(catLookupPath);
if isfield(loadedCat, 'categoryLookupTable')
    catTable = loadedCat.categoryLookupTable;
else
    vars = fieldnames(loadedCat);
    catTable = loadedCat.(vars{1});
end

numProducts = height(mainTable);
numCategories = max(catTable.unique_category_id);
fprintf('Building sparse matrix: %d rows x %d cols\n', numProducts, numCategories);

% Flatten category_ids and build row indices
J = [mainTable.category_ids{:}]';
counts = cellfun(@length, mainTable.category_ids);
I = repelem(1:numProducts, counts)';

sparseMatrix = sparse(I, J, 1, numProducts, numCategories);
nonZeros = nnz(sparseMatrix);
density = (nonZeros / (numProducts * numCategories)) * 100;
fprintf('Non-zeros: %d. Density: %.4f%%\n', nonZeros, density);

save(outputPath, 'sparseMatrix', 'numProducts', 'numCategories', '-v7.3');
fprintf('Saved %s\n', outputPath);
ok = true;
end

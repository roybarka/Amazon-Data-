% generate_category_sparse_matrix.m
% This script creates a sparse logical matrix (Products x Categories).
% Element (i,j) is 1 if Product i belongs to Category j.
%
% Requirements:
% 1. The "Enriched" Lookup Table (containing 'category_ids' column).
% 2. The Category Lookup Table (to determine total number of categories).

clear; clc;

% =========================================================================
% 1. Load Files
% =========================================================================

% A. Select and Load the Enriched Lookup Table (Rows source)
fprintf('Step 1: Select the ENRICHED Lookup Table (containing category_ids)...\n');
[dataFile, dataPath] = uigetfile('*.mat', 'Select Enriched Lookup Table');
if isequal(dataFile, 0), error('No file selected.'); end
dataFullPath = fullfile(dataPath, dataFile);

fprintf('Loading data file: %s ...\n', dataFile);
loadedData = load(dataFullPath);

% Extract table/struct
if isfield(loadedData, 'lookupTableStruct')
    % Convert struct array to table for easier processing
    mainTable = struct2table(loadedData.lookupTableStruct);
elseif isfield(loadedData, 'lookupTable')
    mainTable = loadedData.lookupTable;
elseif isfield(loadedData, 'mainTable')
    mainTable = loadedData.mainTable;
else
    % Fallback: try to find any variable that looks like the data
    vars = fieldnames(loadedData);
    mainTable = loadedData.(vars{1});
    if isstruct(mainTable), mainTable = struct2table(mainTable); end
end

% Verify column exists
if ~ismember('category_ids', mainTable.Properties.VariableNames)
    error('The selected file does not have a "category_ids" column. Please run the enrichment script first.');
end


% B. Select and Load the Category Lookup Table (Columns source)
fprintf('\nStep 2: Select the Category Lookup Table (to define Matrix dimensions)...\n');
[catFile, catPath] = uigetfile('*.mat', 'Select Category Lookup Table');
if isequal(catFile, 0), error('No file selected.'); end

fprintf('Loading category file: %s ...\n', catFile);
loadedCat = load(fullfile(catPath, catFile));
% Assume standard variable name from previous scripts
if isfield(loadedCat, 'categoryLookupTable')
    catTable = loadedCat.categoryLookupTable;
else
    vars = fieldnames(loadedCat);
    catTable = loadedCat.(vars{1});
end

% =========================================================================
% 2. Matrix Construction (Vectorized)
% =========================================================================
fprintf('\n=== Constructing Sparse Matrix ===\n');

numProducts = height(mainTable);
numCategories = max(catTable.unique_category_id); % Determine total columns

fprintf('Target Dimensions: %d Rows (Products) x %d Columns (Categories)\n', numProducts, numCategories);

% 1. Prepare Column Indices (J)
% Concatenate all cell arrays into a single vector.
% 'category_ids' is a cell array where each cell contains a vector of IDs.
% We flatten this structure.
J = [mainTable.category_ids{:}]'; 

% 2. Prepare Row Indices (I)
% We need to repeat the row index 'i' for every category found in row 'i'.
% Calculate how many categories each product has.
counts = cellfun(@length, mainTable.category_ids);

% Create a vector of row indices matching the flattened J vector.
% 'repelem' repeats the index 1 count(1) times, index 2 count(2) times, etc.
I = repelem(1:numProducts, counts)';

% 3. Create Sparse Matrix
% We use 'double' 1s for the values. 
% S will be of size numProducts x numCategories.
fprintf('Building sparse matrix object...\n');
sparseMatrix = sparse(I, J, 1, numProducts, numCategories);

% Optional: Convert to logical if you only need binary flag (saves memory)
% sparseMatrix = spfun(@logical, sparseMatrix); 

% Analyze sparsity
nonZeros = nnz(sparseMatrix);
density = (nonZeros / (numProducts * numCategories)) * 100;
fprintf('Matrix created. Non-zeros: %d. Density: %.4f%%\n', nonZeros, density);


% =========================================================================
% 3. Save Output
% =========================================================================
[~, baseName, ~] = fileparts(dataFile);
outputFileName = sprintf('%s_SparseMatrix.mat', baseName);
outputFullPath = fullfile(dataPath, outputFileName);

fprintf('\nSaving to %s ...\n', outputFileName);

% Save the matrix and the auxiliary data (dimensions, maybe mapping) if needed
save(outputFullPath, 'sparseMatrix', 'numProducts', 'numCategories', '-v7.3');

fprintf('Done.\n');
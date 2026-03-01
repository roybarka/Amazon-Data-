% Validate_Category_Mapping_Report.m
% Cross-validates category-to-ID mapping and SparseMatrix for Magazine_Subscriptions.
%
% Uses:
%   1. meta_Magazine_Subscriptions_part1.mat  (source: parent_asin -> categories)
%   2. meta_Magazine_Subscriptions_Category_Lookup.mat (string -> unique_category_id)
%   3. Magazine_SubscriptionsLookUpTable_Enriched.mat (row index <-> product/parent_asin)
%   4. *_SparseMatrix.mat (rows = products, cols = category IDs)
%
% Run from MATLAB; optionally set path variables below, or use file dialogs.

function Validate_Category_Mapping_Report()
%% Optional: set paths if files are in known locations (otherwise dialogs will open)
metaPartFile   = '';  % e.g. '...\Meta_Data_Mat_files\meta_Magazine_Subscriptions_part1.mat'
categoryLookupFile = '';  % e.g. '...\Meta_Data_LookUp_Table\meta_Magazine_Subscriptions_Category_Lookup.mat'
enrichedTableFile = '';   % e.g. '...\LookUp_Table\Magazine_SubscriptionsLookUpTable_Enriched.mat'
sparseMatrixFile  = '';   % e.g. '...\LookUp_Table\Magazine_SubscriptionsLookUpTable_Enriched_SparseMatrix.mat'

%% Load files (prompt if path empty)
if isempty(metaPartFile)
    [f, p] = uigetfile('*.mat', 'Select Meta Data part file (e.g. meta_Magazine_Subscriptions_part1.mat)');
    if isequal(f, 0), error('Aborted.'); end
    metaPartFile = fullfile(p, f);
end
if isempty(categoryLookupFile)
    [f, p] = uigetfile('*.mat', 'Select Category Lookup (e.g. meta_Magazine_Subscriptions_Category_Lookup.mat)');
    if isequal(f, 0), error('Aborted.'); end
    categoryLookupFile = fullfile(p, f);
end
if isempty(enrichedTableFile)
    [f, p] = uigetfile('*.mat', 'Select Enriched Lookup Table (needed for row <-> product mapping)');
    if isequal(f, 0), error('Aborted.'); end
    enrichedTableFile = fullfile(p, f);
end
if isempty(sparseMatrixFile)
    [f, p] = uigetfile('*.mat', 'Select SparseMatrix .mat file');
    if isequal(f, 0), error('Aborted.'); end
    sparseMatrixFile = fullfile(p, f);
end

%% Load data
fprintf('Loading: %s\n', metaPartFile);
metaLoad = load(metaPartFile);
fnMeta = fieldnames(metaLoad);
metaStruct = metaLoad.(fnMeta{1});

fprintf('Loading: %s\n', categoryLookupFile);
catLoad = load(categoryLookupFile);
if isfield(catLoad, 'categoryLookupTable')
    catTable = catLoad.categoryLookupTable;
else
    fn = fieldnames(catLoad);
    catTable = catLoad.(fn{1});
end

fprintf('Loading: %s\n', enrichedTableFile);
enrLoad = load(enrichedTableFile);
if isfield(enrLoad, 'lookupTableStruct')
    enrTable = struct2table(enrLoad.lookupTableStruct);
elseif isfield(enrLoad, 'lookupTable')
    enrTable = enrLoad.lookupTable;
else
    fn = fieldnames(enrLoad);
    enrTable = enrLoad.(fn{1});
    if isstruct(enrTable), enrTable = struct2table(enrTable); end
end

fprintf('Loading: %s\n', sparseMatrixFile);
smatLoad = load(sparseMatrixFile);
if isfield(smatLoad, 'sparseMatrix')
    sparseMatrix = smatLoad.sparseMatrix;
else
    fn = fieldnames(smatLoad);
    sparseMatrix = smatLoad.(fn{1});
end

%% Build category string -> unique_category_id map (as in Enrich_Lookup_Table)
if istable(catTable)
    cats = catTable.category;
    ids  = catTable.unique_category_id;
else
    cats = catTable.category;
    ids  = catTable.unique_category_id;
end
if iscell(cats), catMap = containers.Map(cats, num2cell(ids)); else, catMap = containers.Map(cellstr(cats), num2cell(ids)); end

%% Build parent_asin -> category strings from META (first occurrence only)
parentToCatStrings = containers.Map('KeyType', 'char', 'ValueType', 'any');
for k = 1:length(metaStruct)
    p_asin = metaStruct(k).parent_asin;
    cats_raw = metaStruct(k).categories;
    if isempty(p_asin) || isempty(cats_raw), continue; end
    if ischar(cats_raw), cats_raw = {cats_raw}; end
    if isstring(cats_raw), cats_raw = cellstr(cats_raw); end
    if ~isKey(parentToCatStrings, p_asin)
        parentToCatStrings(p_asin) = cats_raw;
    end
end

%% Enriched table: row index = matrix row index (1-based)
% Ensure required columns exist
req = {'parent_asin', 'category_ids', 'unique_id'};
for r = 1:length(req)
    if ~ismember(req{r}, enrTable.Properties.VariableNames)
        error('Enriched table missing variable: %s', req{r});
    end
end

% Rows in enriched table that have parent_asin and non-empty category_ids
hasParent = ~cellfun(@isempty, enrTable.parent_asin);
hasCats   = cellfun(@(c) ~isempty(c), enrTable.category_ids);
validRows = find(hasParent & hasCats);

% Restrict to parent_asins that also appear in Meta (so we can cross-check)
validRowsInMeta = false(size(validRows));
for i = 1:length(validRows)
    r = validRows(i);
    p_asin = enrTable.parent_asin{r};
    if isKey(parentToCatStrings, p_asin)
        validRowsInMeta(i) = true;
    end
end
validRows = validRows(validRowsInMeta);

if isempty(validRows)
    error('No enriched rows found with parent_asin in Meta and non-empty category_ids. Cannot run validation.');
end
if length(validRows) < 5
    fprintf('Only %d products have both parent_asin in Meta and non-empty category_ids. Using all.\n', length(validRows));
    nSample = length(validRows);
else
    nSample = 200;
end

% Pick 5 random rows (or all if fewer)
idx = randperm(length(validRows), min(nSample, length(validRows)));
sampleRows = validRows(idx);

%% Validation report
fprintf('\n');
fprintf('========================================\n');
fprintf('  CATEGORY MAPPING VALIDATION REPORT\n');
fprintf('========================================\n\n');

% Code-audit summary
fprintf('--- Indexing (Code Audit) ---\n');
fprintf('  Matrix rows: 1-based; row i = row i of Enriched Lookup Table (product i).\n');
fprintf('  Matrix cols: 1-based; column j = unique_category_id j from Category_Lookup.\n');
fprintf('  SparseMatrix(i,j)=1 means product (table row i) belongs to category j.\n');
fprintf('  unique_category_id in Category_Lookup is 1-based (first category = 1).\n\n');

nProducts = height(enrTable);
nCategories = max(catTable.unique_category_id);
fprintf('  Enriched table rows: %d. SparseMatrix size: %d x %d.\n', nProducts, size(sparseMatrix,1), size(sparseMatrix,2));
if nProducts ~= size(sparseMatrix, 1)
    fprintf('  WARNING: Row count mismatch. Enriched table has %d rows but SparseMatrix has %d rows.\n', nProducts, size(sparseMatrix,1));
end
if nCategories ~= size(sparseMatrix, 2)
    fprintf('  WARNING: Column count mismatch. max(unique_category_id)=%d but SparseMatrix has %d columns.\n', nCategories, size(sparseMatrix,2));
end
fprintf('\n');

fprintf('--- Consistency checks (5 random products) ---\n\n');

allPass = true;
for s = 1:length(sampleRows)
    rowIdx = sampleRows(s);
    parent_asin = enrTable.parent_asin{rowIdx};
    unique_id   = enrTable.unique_id(rowIdx);
    if iscell(unique_id), unique_id = unique_id{1}; end
    if isempty(parent_asin) || ~ischar(parent_asin), parent_asin = char(parent_asin); end

    % Category IDs from Enriched table
    catIdsEnr = enrTable.category_ids{rowIdx};
    catIdsEnr = catIdsEnr(:)';

    % Category strings from Meta
    catStringsMeta = parentToCatStrings(parent_asin);

    % Convert Meta strings to IDs via Category_Lookup
    catIdsFromMeta = [];
    for c = 1:length(catStringsMeta)
        str = catStringsMeta{c};
        if isKey(catMap, str)
            id = catMap(str);
            if iscell(id), id = id{1}; end
            catIdsFromMeta(end+1) = id; %#ok<AGROW>
        end
    end

    % Compare ID sets (order may differ)
    idsMatch = isempty(setxor(catIdsEnr, catIdsFromMeta));

    % Matrix check: for each category ID in Enriched, SparseMatrix(rowIdx, id) should be 1
    matrixOk = true;
    for j = 1:length(catIdsEnr)
        cid = catIdsEnr(j);
        if rowIdx > size(sparseMatrix,1) || cid > size(sparseMatrix,2)
            matrixOk = false;
            break
        end
        val = full(sparseMatrix(rowIdx, cid));
        if val ~= 1
            matrixOk = false;
            break
        end
    end

    pass = idsMatch && matrixOk;
    if ~pass, allPass = false; end

    fprintf('  Product %d (row %d, unique_id %s, parent_asin %s):\n', s, rowIdx, num2str(unique_id), parent_asin);
    fprintf('    Categories (Meta): %s\n', strjoin(catStringsMeta, ', '));
    fprintf('    Category IDs (Lookup): %s\n', mat2str(sort(catIdsFromMeta)));
    fprintf('    Category IDs (Enriched): %s\n', mat2str(sort(catIdsEnr)));
    fprintf('    Meta vs Enriched IDs match: %s\n', iif(idsMatch, 'YES', 'NO'));
    fprintf('    SparseMatrix(row,%s) all 1: %s\n', mat2str(catIdsEnr), iif(matrixOk, 'YES', 'NO'));
    if ~matrixOk
        for j = 1:length(catIdsEnr)
            cid = catIdsEnr(j);
            val = full(sparseMatrix(rowIdx, cid));
            fprintf('      SparseMatrix(%d,%d) = %g\n', rowIdx, cid, val);
        end
    end
    fprintf('    --> %s\n\n', iif(pass, 'PASS', 'FAIL'));
end

fprintf('========================================\n');
if allPass
    fprintf('  OVERALL: All checks PASSED.\n');
else
    fprintf('  OVERALL: One or more checks FAILED. See details above.\n');
end
fprintf('========================================\n');

end

function out = iif(cond, yes, no)
if cond, out = yes; else, out = no; end
end

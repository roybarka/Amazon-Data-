function ok = Enrich_Lookup_Table(cfg)
% ENRICH_LOOKUP_TABLE  Add parent_asin and category_ids to the base Lookup Table.
% Fully automated: no GUI. Uses config with Category -> File Type paths.
%
% cfg.categoryRoot, cfg.categoryName.
% Reads: LookUp_Table/<Category>LookUpTable.mat, Processed_MAT_files/*.mat,
%        Meta_Data_LookUp_Table/meta_<Category>_Category_Lookup.mat, Meta_Data_Mat_files/meta_<Category>_part*.mat.
% Writes: LookUp_Table/<Category>LookUpTable_Enriched.mat.

ok = false;
if nargin < 1 || ~isstruct(cfg)
    error('Enrich_Lookup_Table requires config struct cfg with categoryRoot, categoryName.');
end

paths = pipeline_getCategoryPaths(cfg.categoryRoot, cfg.categoryName);
categoryName = cfg.categoryName;
targetFullPath = paths.lookupTableFile;
outputFile = paths.lookupTableEnrichedFile;
matFilesDir = paths.Processed_MAT_files;
catLookupFile = paths.metaCategoryLookupFile;
metaPath = paths.Meta_Data_Mat_files;
metaPrefix = ['meta_' categoryName];

if ~exist(targetFullPath, 'file')
    error('Base lookup table not found: %s', targetFullPath);
end
if ~exist(catLookupFile, 'file')
    error('Category lookup not found: %s', catLookupFile);
end
if ~exist(metaPath, 'dir')
    error('Meta_Data_Mat_files folder not found: %s', metaPath);
end

% Load target table
tmp = load(targetFullPath);
if isfield(tmp, 'lookupTableStruct')
    mainTable = struct2table(tmp.lookupTableStruct);
elseif isfield(tmp, 'lookupTable')
    mainTable = tmp.lookupTable;
else
    error('Unknown format in lookup table file.');
end
clear tmp;

if ~ismember('parent_asin', mainTable.Properties.VariableNames)
    mainTable.parent_asin = repmat({''}, height(mainTable), 1);
end
if ~ismember('category_ids', mainTable.Properties.VariableNames)
    mainTable.category_ids = cell(height(mainTable), 1);
end

% Load category lookup (string -> ID)
catData = load(catLookupFile);
catTable = catData.categoryLookupTable;
catMap = containers.Map(catTable.category, catTable.unique_category_id);
clear catData catTable;

% Phase A: Populate parent_asin from Processed_MAT_files (or Mat_Files if processed empty)
if ~exist(matFilesDir, 'dir') || isempty(dir(fullfile(matFilesDir, '*.mat')))
    matFilesDir = paths.Mat_Files;
end
if exist(matFilesDir, 'dir')
    dataFiles = dir(fullfile(matFilesDir, '*.mat'));
    for i = 1:length(dataFiles)
        currentFile = fullfile(dataFiles(i).folder, dataFiles(i).name);
        try
            loadedChunk = load(currentFile);
            fNames = fieldnames(loadedChunk);
            chunkData = loadedChunk.(fNames{1});
            if ~isfield(chunkData, 'unique_ids') || ~isfield(chunkData, 'parent_asins')
                continue;
            end
            ids_in_chunk = [chunkData.unique_ids];
            parents_in_chunk = {chunkData.parent_asins};
            uidCol = mainTable.unique_id;
            if iscell(uidCol), uidCol = cell2mat(uidCol); end
            [validMask, loc] = ismember(ids_in_chunk, uidCol);
            foundIndices = loc(validMask);
            sourceIndices = find(validMask);
            mainTable.parent_asin(foundIndices) = parents_in_chunk(sourceIndices)';
        catch
            % skip file
        end
        clear loadedChunk chunkData ids_in_chunk parents_in_chunk validMask loc foundIndices sourceIndices;
    end
end

% Phase B: Build Parent -> Categories map from Meta_Data_Mat_files
parentToCatsMap = containers.Map('KeyType', 'char', 'ValueType', 'any');
metaFiles = dir(fullfile(metaPath, [metaPrefix '_part*.mat']));
for i = 1:length(metaFiles)
    mFile = fullfile(metaFiles(i).folder, metaFiles(i).name);
    try
        loadedMeta = load(mFile);
        fNames = fieldnames(loadedMeta);
        metaStruct = loadedMeta.(fNames{1});
        for k = 1:length(metaStruct)
            p_asin = metaStruct(k).parent_asin;
            cats_raw = metaStruct(k).categories;
            if isempty(p_asin) || isempty(cats_raw), continue; end
            if ischar(cats_raw), cats_raw = {cats_raw}; end
            if isstring(cats_raw), cats_raw = cellstr(cats_raw); end
            currentIDs = [];
            for c = 1:length(cats_raw)
                cName = cats_raw{c};
                if isKey(catMap, cName)
                    currentIDs(end+1) = catMap(cName); %#ok<SAGROW>
                end
            end
            if ~isempty(currentIDs)
                parentToCatsMap(p_asin) = currentIDs;
            end
        end
    catch
        % skip file
    end
    clear loadedMeta metaStruct;
end

% Phase C: Merge categories into main table
hasParent = ~cellfun(@isempty, mainTable.parent_asin);
totalRows = height(mainTable);
for r = 1:totalRows
    if hasParent(r)
        p_asin = mainTable.parent_asin{r};
        if isKey(parentToCatsMap, p_asin)
            mainTable.category_ids{r} = parentToCatsMap(p_asin);
        end
    end
end

% Save
if ~exist(paths.LookUp_Table, 'dir')
    mkdir(paths.LookUp_Table);
end
lookupTableStruct = table2struct(mainTable); %#ok<NASGU>
save(outputFile, 'lookupTableStruct', '-v7.3');
fprintf('Saved Enriched lookup: %s\n', outputFile);
ok = true;
end

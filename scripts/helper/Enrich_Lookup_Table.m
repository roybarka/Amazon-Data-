% Enrich_Lookup_Table.m
% This script enriches an existing Lookup Table with 'parent_asin' and 'category_ids'.
% Flow:
% 1. Load Target Lookup Table.
% 2. Iterate over 'Mat_Files' (Data chunks) to extract parent_asin based on unique_id.
% 3. Load Category Lookup Table (String -> ID map).
% 4. Iterate over 'Meta_Data' files to build a mapping of Parent_ASIN -> Category_IDs.
% 5. Merge all data into the Target Table and save.

clear; clc;

% =========================================================================
% 1. File Selection & Path Setup
% =========================================================================

% A. Select the Target Lookup Table to enrich
fprintf('Step 1: Select the Lookup Table you want to enrich...\n');
[targetFile, targetPath] = uigetfile('*.mat', 'Select Target Lookup Table (e.g., SoftwareLookUpTable.mat)');
if isequal(targetFile, 0), error('No file selected.'); end
targetFullPath = fullfile(targetPath, targetFile);

% Load the target table immediately to understand its size
fprintf('Loading target table: %s...\n', targetFile);
tmp = load(targetFullPath);
% Support both struct format (lookupTableStruct) and table format
if isfield(tmp, 'lookupTableStruct')
    mainTable = struct2table(tmp.lookupTableStruct);
elseif isfield(tmp, 'lookupTable')
    mainTable = tmp.lookupTable;
else
    error('Unknown format in lookup table file.');
end
clear tmp;

% Ensure new columns exist
if ~ismember('parent_asin', mainTable.Properties.VariableNames)
    mainTable.parent_asin = repmat({''}, height(mainTable), 1);
end
if ~ismember('category_ids', mainTable.Properties.VariableNames)
    mainTable.category_ids = cell(height(mainTable), 1);
end


% B. Identify "Mat_Files" Folder (Source of Parent ASIN)
% We assume the folder name matches the product category name found in paths.
% Let's ask the user to point to the specific folder inside Mat_Files to be safe.
fprintf('\nStep 2: Select the folder containing the PROCESSED MAT FILES for this category.\n');
fprintf('(e.g., G:\\AmazonData\\Mat_Files\\Subscription_Boxes)\n');
matFilesDir = uigetdir(targetPath, 'Select Mat_Files Folder');
if isequal(matFilesDir, 0), error('No folder selected.'); end


% C. Identify "Meta_Data" File Pattern (Source of Categories)
% Since names might mismatch (Subscription_Boxes vs meta_Magazine...), we ask the user 
% to pick ONE meta file to deduce the pattern.
fprintf('\nStep 3: Select ONE Meta_Data file associated with this category.\n');
fprintf('(e.g., inside G:\\AmazonData\\Meta_Data Mat_files Tests, select meta_Magazine_Subscriptions_part1.mat)\n');
[metaFile, metaPath] = uigetfile('*.mat', 'Select a Sample Meta Data File');
if isequal(metaFile, 0), error('No file selected.'); end

% Parse the base name from the selected meta file (e.g., "meta_Books")
% We assume pattern is "meta_CategoryName_partX.mat"
[~, metaBaseName, ~] = fileparts(metaFile);
splitName = strsplit(metaBaseName, '_part');
metaPrefix = splitName{1}; % e.g., "meta_Magazine_Subscriptions" or "meta_Books"

fprintf('Meta Data Prefix identified as: %s\n', metaPrefix);


% D. Locate and Load the Category Lookup Table
% We assume it is in "Meta_Data LookUp Table" and named "[metaPrefix]_Category_Lookup.mat"
% Trying to locate the folder relative to the selected metaPath or hardcoded
lookupTableBaseDir = fullfile(fileparts(metaPath), 'Meta_Data LookUp Table');
% If that path doesn't exist, ask user or try a known path
if ~exist(lookupTableBaseDir, 'dir')
    % Try to go up one level from metaPath if it was in "Meta_Data Mat_files Tests"
    lookupTableBaseDir = fullfile(fileparts(fileparts(metaPath)), 'Meta_Data LookUp Table');
end

catLookupFile = fullfile(lookupTableBaseDir, [metaPrefix, '_Category_Lookup.mat']);

if ~exist(catLookupFile, 'file')
    fprintf('Could not auto-locate Category Lookup file at: %s\n', catLookupFile);
    [cFile, cPath] = uigetfile('*.mat', ['Select Category Lookup for ' metaPrefix]);
    if isequal(cFile, 0), error('Category Lookup not found.'); end
    catLookupFile = fullfile(cPath, cFile);
end

fprintf('Loading Category Lookup Table: %s...\n', catLookupFile);
catData = load(catLookupFile);
% Create a Map for fast String -> ID conversion
catTable = catData.categoryLookupTable; 
% Ensure efficient map
catMap = containers.Map(catTable.category, catTable.unique_category_id);
clear catData catTable; % Free RAM


% =========================================================================
% 2. Phase A: Populate 'parent_asin' from Mat_Files
% =========================================================================
fprintf('\n=== Phase A: Populating Parent ASINs from Mat Files ===\n');

dataFiles = dir(fullfile(matFilesDir, '*.mat'));
% Sort files naturally (optional, but good for logs) if needed, but not strictly required
fprintf('Found %d data files. Processing...\n', length(dataFiles));

for i = 1:length(dataFiles)
    currentFile = fullfile(dataFiles(i).folder, dataFiles(i).name);
    fprintf('  Processing (%d/%d): %s ... ', i, length(dataFiles), dataFiles(i).name);
    
    try
        loadedChunk = load(currentFile);
        % Assuming the struct is named 'dataStruct' or 'combinedDataStruct' inside
        fNames = fieldnames(loadedChunk);
        chunkData = loadedChunk.(fNames{1}); 
        
        % Extract vectors
        % Depending on your script, 'chunkData' might be a struct array or struct of arrays.
        % Based on previous scripts, it seems to be a struct array.
        
        % Optimization: Loop through the struct array is slow.
        % Let's assume standard struct array.
        
        % We need to update mainTable where unique_id matches.
        % Since unique_ids are indices (mostly), we can be direct.
        
        ids_in_chunk = [chunkData.unique_ids];
        parents_in_chunk = {chunkData.parent_asins};
        
        % Update Main Table
        % We map IDs to the table rows. 
        % Check if mainTable uses unique_id as direct index or if we need find/ismember.
        % Assuming mainTable.unique_id corresponds to the values:
        
        [validMask, loc] = ismember(ids_in_chunk, mainTable.unique_id);
        
        % Only update found indices
        foundIndices = loc(validMask);
        sourceIndices = find(validMask);
        
        mainTable.parent_asin(foundIndices) = parents_in_chunk(sourceIndices)';
        
        fprintf('Updated %d records.\n', length(foundIndices));
        
    catch ME
        fprintf('Error reading file: %s\n', ME.message);
    end
    
    clear loadedChunk chunkData ids_in_chunk parents_in_chunk; % FREE RAM
end


% =========================================================================
% 3. Phase B: Build Parent -> Categories Map from Meta Data
% =========================================================================
fprintf('\n=== Phase B: Building Parent->Categories Map from Meta Data ===\n');
% We must read ALL meta files to build a mapping, because parents are scattered.
% To save RAM, we only store the resulting Map (Parent -> [IDs]), not the raw data.

parentToCatsMap = containers.Map('KeyType', 'char', 'ValueType', 'any');
metaFiles = dir(fullfile(metaPath, [metaPrefix '_part*.mat']));

fprintf('Found %d meta files. Building map...\n', length(metaFiles));

for i = 1:length(metaFiles)
    mFile = fullfile(metaFiles(i).folder, metaFiles(i).name);
    fprintf('  Scanning (%d/%d): %s ... ', i, length(metaFiles), metaFiles(i).name);
    
    try
        loadedMeta = load(mFile);
        % Usually named 'partialDataStruct' based on your meta script
        fNames = fieldnames(loadedMeta);
        metaStruct = loadedMeta.(fNames{1});
        
        % Iterate through records in this chunk
        for k = 1:length(metaStruct)
            p_asin = metaStruct(k).parent_asin;
            cats_raw = metaStruct(k).categories;
            
            % Skip if no parent asin or no categories
            if isempty(p_asin) || isempty(cats_raw), continue; end
            
            % Convert raw categories to IDs using catMap
            if ischar(cats_raw), cats_raw = {cats_raw}; end
            if isstring(cats_raw), cats_raw = cellstr(cats_raw); end
            
            currentIDs = [];
            for c = 1:length(cats_raw)
                cName = cats_raw{c};
                if isKey(catMap, cName)
                    currentIDs(end+1) = catMap(cName); %#ok<SAGROW>
                end
            end
            
            % Store in Map (Parent -> List of Doubles)
            % Only store if we found valid IDs
            if ~isempty(currentIDs)
                parentToCatsMap(p_asin) = currentIDs;
            end
        end
        
        fprintf('Map size: %d parents.\n', parentToCatsMap.Count);
        
    catch ME
        fprintf('Error reading meta file: %s\n', ME.message);
    end
    
    clear loadedMeta metaStruct; % FREE RAM
end


% =========================================================================
% 4. Phase C: Merge Categories into Main Table
% =========================================================================
fprintf('\n=== Phase C: Merging Categories into Lookup Table ===\n');

% Iterate over the main table
countUpdated = 0;
totalRows = height(mainTable);

% Pre-fetch keys to speed up
hasParent = ~cellfun(@isempty, mainTable.parent_asin);

for r = 1:totalRows
    if hasParent(r)
        p_asin = mainTable.parent_asin{r};
        if isKey(parentToCatsMap, p_asin)
            mainTable.category_ids{r} = parentToCatsMap(p_asin);
            countUpdated = countUpdated + 1;
        end
    end
    
    if mod(r, 10000) == 0
        fprintf('  Processed %d/%d rows...\n', r, totalRows);
    end
end

fprintf('Done. Total rows enriched with categories: %d\n', countUpdated);


% =========================================================================
% 5. Save Result
% =========================================================================

% Get the parent directory of the current targetPath to find the sibling folder
% Handle potential trailing slash
if targetPath(end) == filesep
    tempPath = targetPath(1:end-1);
else
    tempPath = targetPath;
end
[parentDir, ~, ~] = fileparts(tempPath);

% Define the new output directory "LookupTableEnrich"
outputDir = fullfile(parentDir, 'LookupTableEnrich');

% Create the directory if it does not exist
if ~exist(outputDir, 'dir')
    mkdir(outputDir);
    fprintf('Created new directory: %s\n', outputDir);
end

% Construct the output filename
[~, name, ext] = fileparts(targetFullPath);
outputFile = fullfile(outputDir, [name '_Enriched' ext]);

% Convert back to struct if necessary
lookupTableStruct = table2struct(mainTable); %#ok<NASGU>

fprintf('Saving to %s ...\n', outputFile);
save(outputFile, 'lookupTableStruct', '-v7.3');
fprintf('Success. File saved in separate enrichment folder.\n');
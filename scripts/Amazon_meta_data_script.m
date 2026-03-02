function ok = Amazon_meta_data_script(cfg)
% AMAZON_META_DATA_SCRIPT  Read meta_*.jsonl, save chunked .mat and category lookup.
% Fully automated: no GUI. Uses config with Category -> File Type paths.
%
% cfg.categoryRoot, cfg.categoryName, cfg.metaJsonlFolder (folder containing meta_*.jsonl).
% If metaJsonlFolder is empty or missing, returns ok = true without error (skip).
% Writes: Meta_Data_Mat_files/*.mat, Meta_Data_LookUp_Table/*_Category_Lookup.mat.

ok = false;
if nargin < 1 || ~isstruct(cfg)
    error('Amazon_meta_data_script requires config struct cfg.');
end

if ~isfield(cfg, 'metaJsonlFolder') || isempty(cfg.metaJsonlFolder) || ~exist(cfg.metaJsonlFolder, 'dir')
    ok = true;
    return;
end

inputFolder = cfg.metaJsonlFolder;
paths = pipeline_getCategoryPaths(cfg.categoryRoot, cfg.categoryName);
outputFolder = paths.Meta_Data_Mat_files;
lookupTableDir = paths.Meta_Data_LookUp_Table;

if ~exist(outputFolder, 'dir'), mkdir(outputFolder); end
if ~exist(lookupTableDir, 'dir'), mkdir(lookupTableDir); end

logFile = fullfile(outputFolder, sprintf('MetaDataLog_%s.txt', datestr(now,'yyyy-mm-dd_HH-MM-SS')));
logFID = fopen(logFile, 'a');
cleanupLog = onCleanup(@() fclose(logFID));

fprintf(logFID, '=== Amazon MetaData Run %s ===\n', datestr(now));
fprintf(logFID, 'Input: %s\nOutput: %s\n\n', inputFolder, outputFolder);

% When the folder contains meta_*.jsonl for many categories, only process the one for this category.
categoryName = cfg.categoryName;
expectedBaseName = ['meta_' categoryName];

filesAll = dir(fullfile(inputFolder, 'meta_*.jsonl'));
keep = false(size(filesAll));
for ki = 1:numel(filesAll)
    [~, bn] = fileparts(filesAll(ki).name);
    keep(ki) = strcmp(bn, expectedBaseName);
end
files = filesAll(keep);
if isempty(files)
    fprintf('No %s.jsonl in %s. Skipping meta for category %s.\n', expectedBaseName, inputFolder, categoryName);
    ok = true;
    return;
end

for k = 1:numel(files)
    inFile = fullfile(files(k).folder, files(k).name);
    [~, baseName] = fileparts(files(k).name);
    outFile = fullfile(outputFolder, [baseName '.mat']);

    fprintf('(%d/%d) %s\n', k, numel(files), files(k).name);
    fprintf(logFID, '(%d/%d) %s\n', k, numel(files), files(k).name);

    if exist(outFile, 'file') || ~isempty(dir(fullfile(outputFolder, sprintf('%s_part*.mat', baseName))))
        fprintf(logFID, '  -> exists, skipping\n\n');
        continue;
    end

    try
        categoryMap = containers.Map('KeyType', 'char', 'ValueType', 'double');
        parse_and_save_chunked(inFile, outputFolder, baseName, logFID, categoryMap);

        if ~isempty(categoryMap)
            allCats = keys(categoryMap)';
            allIDs = values(categoryMap)';
            allIDs = cell2mat(allIDs);
            categoryLookupTable = table(allIDs, allCats, 'VariableNames', {'unique_category_id', 'category'});
            categoryLookupTable = sortrows(categoryLookupTable, 'unique_category_id');
            tableName = sprintf('%s_Category_Lookup.mat', baseName);
            saveFile = fullfile(lookupTableDir, tableName);
            save(saveFile, 'categoryLookupTable');
            fprintf(logFID, '  -> Saved %s (%d categories)\n', tableName, length(allIDs));
        end
    catch ME
        fprintf(2, '  !! failed: %s\n', ME.message);
        fprintf(logFID, '  !! failed: %s\n\n', ME.message);
    end
end

fprintf(logFID, '=== Done at %s ===\n', datestr(now));
ok = true;
end


function parse_and_save_chunked(inputFilePath, outputFolder, baseName, logFID, categoryMap)
    fid = fopen(inputFilePath, 'r', 'n', 'UTF-8');
    if fid == -1
        error('Failed to open file: %s', inputFilePath);
    end
    cleanupObj = onCleanup(@() fclose(fid));

    emptyRec = struct( ...
        'main_category', [], 'title', [], 'average_rating', [], 'rating_number', [], ...
        'features', [], 'description', [], 'price', [], 'images', [], 'videos', [], ...
        'store', [], 'categories', [], 'details', [], 'parent_asin', [], 'bought_together', []);

    maxRecords = 1e6;
    dataStruct = repmat(emptyRec, 0, 1);
    recCounter = 0;
    chunkCounter = 0;

    fprintf(logFID, '  -> parsing %s\n', inputFilePath);

    while ~feof(fid)
        line = fgetl(fid);
        if ~ischar(line), break; end
        if isempty(line), continue; end
        try
            rec = jsondecode(line);
        catch
            continue;
        end
        % Skip if rec is not a scalar struct (e.g. empty struct array from [] or {})
        if isempty(rec) || numel(rec) ~= 1
            continue;
        end

        recCounter = recCounter + 1;
        tmp = emptyRec;
        tmp.main_category = safeField(rec,'main_category');
        tmp.title = safeField(rec,'title');
        tmp.average_rating = safeField(rec,'average_rating');
        tmp.rating_number = safeField(rec,'rating_number');
        tmp.features = safeField(rec,'features');
        tmp.description = safeField(rec,'description');
        tmp.price = safeField(rec,'price');
        tmp.images = safeField(rec,'images');
        tmp.videos = safeField(rec,'videos');
        tmp.store = safeField(rec,'store');
        tmp.categories = safeField(rec,'categories');
        tmp.details = safeField(rec,'details');
        tmp.parent_asin = safeField(rec,'parent_asin');
        tmp.bought_together = safeField(rec,'bought_together');
        dataStruct(end+1) = tmp; %#ok<SAGROW>

        if ~isempty(tmp.categories)
            currentCats = tmp.categories;
            if ischar(currentCats), currentCats = {currentCats}; elseif isstring(currentCats), currentCats = cellstr(currentCats); end
            for cIdx = 1:length(currentCats)
                catStr = currentCats{cIdx};
                if ~isKey(categoryMap, catStr)
                    categoryMap(catStr) = categoryMap.Count + 1;
                end
            end
        end

        if mod(recCounter, 10000) == 0
            fprintf(logFID, '    parsed %d records...\n', recCounter);
        end

        if mod(recCounter, maxRecords) == 0
            chunkCounter = chunkCounter + 1;
            chunkOutFile = fullfile(outputFolder, sprintf('%s_part%d.mat', baseName, chunkCounter));
            partialDataStruct = dataStruct; %#ok<NASGU>
            save(chunkOutFile, 'partialDataStruct', '-v7.3');
            fprintf(logFID, '  -> saved chunk %d\n', chunkCounter);
            dataStruct = repmat(emptyRec, 0, 1);
        end
    end

    if ~isempty(dataStruct)
        chunkCounter = chunkCounter + 1;
        chunkOutFile = fullfile(outputFolder, sprintf('%s_part%d.mat', baseName, chunkCounter));
        partialDataStruct = dataStruct; %#ok<NASGU>
        save(chunkOutFile, 'partialDataStruct', '-v7.3');
        fprintf(logFID, '  -> saved final chunk %d\n', chunkCounter);
    end
    fprintf(logFID, '  Total records: %d\n\n', recCounter);
end


function val = safeField(rec, fieldName)
    if isempty(rec) || numel(rec) ~= 1
        val = [];
        return;
    end
    if isfield(rec, fieldName)
        val = rec.(fieldName);
    else
        val = [];
    end
end

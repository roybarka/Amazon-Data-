function ok = Amazon_data_script(cfg)
% AMAZON_DATA_SCRIPT  Read JSONL file, process data, save chunked .mat and lookup table.
% Fully automated: no GUI. Uses config struct with Category -> File Type paths.
%
% cfg.categoryRoot   - root containing category folders (e.g. 'F:\Amazon_data Part 2')
% cfg.categoryName   - category folder name (e.g. 'Digital_Music')
% cfg.jsonlFilePath  - full path to the .jsonl review file (required)
% cfg.chunkSize     - (optional) items per chunk; default 10000
%
% Output: Mat_Files/*.mat and LookUp_Table/<Category>LookUpTable.mat under category folder.

ok = false;
if nargin < 1 || ~isstruct(cfg)
    error('Amazon_data_script requires a config struct cfg with categoryRoot, categoryName, jsonlFilePath.');
end

categoryRoot = cfg.categoryRoot;
categoryName = cfg.categoryName;
inputFilePath = cfg.jsonlFilePath;
if isempty(inputFilePath) || ~exist(inputFilePath, 'file')
    error('cfg.jsonlFilePath must be a valid path to a .jsonl file.');
end

paths = pipeline_getCategoryPaths(categoryRoot, categoryName);
if ~exist(paths.Mat_Files, 'dir'), mkdir(paths.Mat_Files); end
if ~exist(paths.LookUp_Table, 'dir'), mkdir(paths.LookUp_Table); end

outputFileName = paths.matFilesBaseName;  % base name for output .mat files
lookupTableFileName = paths.lookupTableFile;

chunkSize = 10000;
if isfield(cfg, 'chunkSize') && isnumeric(cfg.chunkSize) && cfg.chunkSize > 0
    chunkSize = cfg.chunkSize;
end

asinSet = containers.Map('KeyType', 'char', 'ValueType', 'logical');
asinIndexMap = containers.Map('KeyType', 'char', 'ValueType', 'double');
lookupTable = containers.Map('KeyType', 'double', 'ValueType', 'char');
counter = 0;
uniqueIDCounter = 0;
ratingAdded = true;
fid = fopen(inputFilePath, 'r');
if fid == -1
    error('Failed to open file: %s', inputFilePath);
end
cleanupFid = onCleanup(@() fclose(fid));

while ratingAdded
    dataStruct = struct('ratings', {}, 'titles', {}, 'texts', {}, 'timestamps', {}, 'parent_asins', {}, 'user_ids', {}, 'verified_purchases', {}, 'helpful_votes', {}, 'images', {}, 'asins', {}, 'unique_ids', {});
    asinIndexMap = containers.Map('KeyType', 'char', 'ValueType', 'double');
    ratingAdded = false;
    counter = counter + 1;

    fseek(fid, 0, 'bof');
    iteration_counter = 0;

    while ~feof(fid)
        line = fgetl(fid);
        if line == -1
            break;
        end
        iteration_counter = iteration_counter + 1;
        data = jsondecode(line);
        asin = data.asin;

        if isKey(asinIndexMap, asin)
            idx = asinIndexMap(asin);
            dataStruct(idx).ratings(end+1) = data.rating;
            dataStruct(idx).titles{end+1} = data.title;
            dataStruct(idx).texts{end+1} = data.text;
            dataStruct(idx).timestamps(end+1) = data.timestamp / 1000;
            dataStruct(idx).user_ids{end+1} = data.user_id;
            dataStruct(idx).verified_purchases(end+1) = data.verified_purchase;
            dataStruct(idx).helpful_votes(end+1) = data.helpful_vote;
            dataStruct(idx).images{end+1} = data.images;
        elseif length(dataStruct) < chunkSize && ~isKey(asinSet, asin)
            ratingAdded = true;
            uniqueIDCounter = uniqueIDCounter + 1;
            asinSet(asin) = true;
            idx = length(dataStruct) + 1;
            asinIndexMap(asin) = idx;
            dataStruct(idx).ratings = data.rating;
            dataStruct(idx).titles = {data.title};
            dataStruct(idx).texts = {data.text};
            dataStruct(idx).timestamps = data.timestamp / 1000;
            dataStruct(idx).parent_asins = data.parent_asin;
            dataStruct(idx).user_ids = {data.user_id};
            dataStruct(idx).verified_purchases = data.verified_purchase;
            dataStruct(idx).helpful_votes = data.helpful_vote;
            dataStruct(idx).images = {data.images};
            dataStruct(idx).asins = asin;
            dataStruct(idx).unique_ids = uniqueIDCounter;
            lookupTable(uniqueIDCounter) = asin;
        end
    end

    for k = 1:length(dataStruct)
        [~, sortIdx] = sort(dataStruct(k).timestamps);
        dataStruct(k).ratings = dataStruct(k).ratings(sortIdx);
        dataStruct(k).titles = dataStruct(k).titles(sortIdx);
        dataStruct(k).texts = dataStruct(k).texts(sortIdx);
        dataStruct(k).timestamps = dataStruct(k).timestamps(sortIdx);
        dataStruct(k).user_ids = dataStruct(k).user_ids(sortIdx);
        dataStruct(k).verified_purchases = dataStruct(k).verified_purchases(sortIdx);
        dataStruct(k).helpful_votes = dataStruct(k).helpful_votes(sortIdx);
        dataStruct(k).images = dataStruct(k).images(sortIdx);
    end

    if ~isempty(dataStruct)
        startIdx = (counter - 1) * chunkSize + 1;
        endIdx = startIdx + chunkSize - 1;
        saveFileName = sprintf('%s%d_%d.mat', outputFileName, startIdx, endIdx);
        fprintf('Saving %s at %s (reviews: %d)\n', saveFileName, char(datetime("now")), iteration_counter);
        save(saveFileName, 'dataStruct', '-v7.3');
    end
end

lookupTableKeys = keys(lookupTable);
lookupTableValues = values(lookupTable);
lookupTableStruct = struct('unique_id', lookupTableKeys, 'asin', lookupTableValues);
save(lookupTableFileName, 'lookupTableStruct', '-v7.3');
fprintf('Saved lookup table %s\n', lookupTableFileName);

ok = true;
end

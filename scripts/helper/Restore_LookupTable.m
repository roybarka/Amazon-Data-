function ok = Restore_LookupTable(cfg)
% RESTORE_LOOKUPTABLE  Build LookUp table (unique_id <-> asin) from Mat_Files for one category.
% Fully automated: no GUI. Uses config with Category -> File Type paths.
%
% cfg.categoryRoot, cfg.categoryName.
% Reads: Mat_Files/*.mat. Writes: LookUp_Table/<Category>LookUpTable.mat.

ok = false;
if nargin < 1 || ~isstruct(cfg)
    error('Restore_LookupTable requires config struct cfg with categoryRoot, categoryName.');
end

paths = pipeline_getCategoryPaths(cfg.categoryRoot, cfg.categoryName);
folderPath = paths.Mat_Files;
outputFullPath = paths.lookupTableFile;
categoryName = cfg.categoryName;

if ~exist(folderPath, 'dir')
    error('Mat_Files directory does not exist: %s', folderPath);
end
if ~exist(paths.LookUp_Table, 'dir'), mkdir(paths.LookUp_Table); end

if exist(outputFullPath, 'file')
    fprintf('LookUp table already exists: %s. Skipping.\n', outputFullPath);
    ok = true;
    return;
end

files = dir(fullfile(folderPath, '*.mat'));
fileNames = {files.name};
if isempty(fileNames)
    fprintf('No .mat files in %s. Skipping.\n', categoryName);
    ok = true;
    return;
end

firstNums = zeros(1, numel(fileNames));
for f = 1:numel(fileNames)
    tokens = regexp(fileNames{f}, '\d+', 'match');
    if ~isempty(tokens)
        firstNums(f) = str2double(tokens{1});
    end
end
[~, idx] = sort(firstNums);
fileNames = fileNames(idx);

all_unique_ids = [];
all_asins = {};

for i = 1:numel(fileNames)
    filePath = fullfile(folderPath, fileNames{i});
    try
        loadedData = load(filePath);
        varNames = fieldnames(loadedData);
        currentStruct = loadedData.(varNames{1});
        if isfield(currentStruct, 'unique_ids')
            batch_ids = [currentStruct.unique_ids];
            all_unique_ids = [all_unique_ids, batch_ids]; %#ok<AGROW>
        end
        if isfield(currentStruct, 'asins')
            batch_asins = {currentStruct.asins};
            all_asins = [all_asins, batch_asins]; %#ok<AGROW>
        end
        if mod(i, 5) == 0 || i == numel(fileNames)
            fprintf('  Processed file %d/%d: %s\n', i, numel(fileNames), fileNames{i});
        end
    catch ME
        fprintf('  Error reading %s: %s\n', fileNames{i}, ME.message);
    end
end

if isempty(all_unique_ids)
    fprintf('  No data for %s. Skipping save.\n', categoryName);
    ok = true;
    return;
end

numItems = length(all_unique_ids);
idsCell = num2cell(all_unique_ids);
lookupTableStruct = struct('unique_id', idsCell, 'asin', all_asins);
save(outputFullPath, 'lookupTableStruct', '-v7.3');
fprintf('  Saved: %s\n', outputFullPath);
ok = true;
end

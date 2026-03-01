% Amazon_meta_batch_chunked_log.m
% Batch: read all META JSONL files and save each as .mat chunks (~1M records)
% with progress logging and skip-existing logic

clear; clc;

% --- Choose input folder (starts one level up, in Meta_Data) ---
inputFolder = uigetdir('..\Meta_Data', 'Select the Meta_Data folder');
if isequal(inputFolder,0)
    error('No folder selected.');
end
fprintf('Input folder: %s\n', inputFolder);

% --- Output folder (fixed path) ---
outputFolder = 'G:\AmazonData\Meta_Data Mat_files';
if ~exist(outputFolder, 'dir')
    mkdir(outputFolder);
end
fprintf('Output folder: %s\n\n', outputFolder);

% --- Lookup Table Setup ---
lookupTableDir = 'G:\AmazonData\Meta_Data LookUp Table'; 
if ~exist(lookupTableDir, 'dir')
    mkdir(lookupTableDir);
end


% --- Logging setup ---
logFile = fullfile(outputFolder, ...
    sprintf('MetaDataLog_%s.txt', datestr(now,'yyyy-mm-dd_HH-MM-SS')));
logFID = fopen(logFile,'a');
cleanupLog = onCleanup(@() fclose(logFID));

fprintf(logFID, '=== Amazon MetaData Batch Run %s ===\n', datestr(now));
fprintf(logFID, 'Input folder: %s\nOutput folder: %s\n\n', inputFolder, outputFolder);

% --- File list ---
files = dir(fullfile(inputFolder, 'meta_*.jsonl'));
if isempty(files)
    error('No meta_*.jsonl files found in: %s', inputFolder);
end

% --- Process each file ---
for k = 1:numel(files)
    inFile = fullfile(files(k).folder, files(k).name);
    [~, baseName] = fileparts(files(k).name);
    outFile = fullfile(outputFolder, [baseName '.mat']);

    fprintf('(%d/%d) %s\n', k, numel(files), files(k).name);
    fprintf(logFID, '(%d/%d) %s\n', k, numel(files), files(k).name);

    % Skip if output already exists
    if exist(outFile, 'file') || ~isempty(dir(fullfile(outputFolder, sprintf('%s_part*.mat', baseName))))
        msg = sprintf('  -> exists, skipping: %s\n\n', outFile);
        fprintf(msg);
        fprintf(logFID, msg);
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
            
            % Generate unique filename using baseName
            tableName = sprintf('%s_Category_Lookup.mat', baseName);
            saveFile = fullfile(lookupTableDir, tableName);
            
            save(saveFile, 'categoryLookupTable');
            
            msg = sprintf('  -> Saved category table: %s (%d categories)\n', tableName, length(allIDs));
            fprintf(msg);
            fprintf(logFID, msg);
        end
    catch ME
        msg = sprintf('  !! failed: %s\n      %s\n\n', inFile, ME.message);
        fprintf(2, msg);
        fprintf(logFID, msg);
    end
end

% --- Save Category Lookup Table ---
fprintf('Saving Category Lookup Table...\n');
if ~isempty(categoryMap)
    allCats = keys(categoryMap)';
    allIDs = values(categoryMap)';
    allIDs = cell2mat(allIDs); 
    
    categoryLookupTable = table(allIDs, allCats, 'VariableNames', {'unique_category_id', 'category'});
    categoryLookupTable = sortrows(categoryLookupTable, 'unique_category_id');

    tableName = sprintf('%s_Category_Lookup.mat', baseName);
    saveFile = fullfile(lookupTableDir, tableName);
    save(saveFile, 'categoryLookupTable');

    fprintf('Category Lookup Table saved with %d unique categories.\n', length(allIDs));
else
    fprintf('No categories found to save.\n');
end

fprintf('Done.\n');
fprintf(logFID, '=== Done at %s ===\n', datestr(now));



% =====================================================
% Helper: parse JSONL file and save every 1M records
% =====================================================
function parse_and_save_chunked(inputFilePath, outputFolder, baseName, logFID, categoryMap)
    % Open file
    fid = fopen(inputFilePath, 'r','n','UTF-8');
    if fid == -1
        error('Failed to open file: %s', inputFilePath);
    end
    cleanupObj = onCleanup(@() fclose(fid));

    % Struct template
    emptyRec = struct( ...
        'main_category', [], ...
        'title', [], ...
        'average_rating', [], ...
        'rating_number', [], ...
        'features', [], ...
        'description', [], ...
        'price', [], ...
        'images', [], ...
        'videos', [], ...
        'store', [], ...
        'categories', [], ...
        'details', [], ...
        'parent_asin', [], ...
        'bought_together', [] ...        
    );

    maxRecords = 1e6;          % records per chunk
    dataStruct = repmat(emptyRec, 0, 1);
    recCounter = 0;
    chunkCounter = 0;

    fprintf('  -> parsing %s\n', inputFilePath);
    fprintf(logFID, '  -> parsing %s\n', inputFilePath);

    while ~feof(fid)
        line = fgetl(fid);
        if ~ischar(line), break; end
        if isempty(line), continue; end

        try
            rec = jsondecode(line);
        catch
            continue;  % skip malformed line
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

        % --- Update Category Map ---
        if ~isempty(tmp.categories)
            currentCats = tmp.categories;
            % Ensure format is a cell array of strings
            if ischar(currentCats)
                currentCats = {currentCats};
            elseif isstring(currentCats)
                currentCats = cellstr(currentCats);
            end
            
            % Iterate through categories and add to map if new
            for cIdx = 1:length(currentCats)
                catStr = currentCats{cIdx};
                if ~isKey(categoryMap, catStr)
                    % Assign new ID based on current count
                    newID = categoryMap.Count + 1;
                    categoryMap(catStr) = newID;
                end
            end
        end

        % Progress
        if mod(recCounter, 10000) == 0
            msg = sprintf('    parsed %d records...\n', recCounter);
            fprintf(msg);
            fprintf(logFID, msg);
        end

        % Save and clear every million records
        if mod(recCounter, maxRecords) == 0
            chunkCounter = chunkCounter + 1;
            chunkOutFile = fullfile(outputFolder, sprintf('%s_part%d.mat', baseName, chunkCounter));
            msg = sprintf('  -> saving chunk %d (%d records) at %s\n', ...
                chunkCounter, numel(dataStruct), datestr(now,'yyyy-mm-dd HH:MM:SS'));
            fprintf(msg);
            fprintf(logFID, msg);

            partialDataStruct = dataStruct; %#ok<NASGU>
            save(chunkOutFile, 'partialDataStruct', '-v7.3');

            msg = sprintf('  -> saved: %s\n', chunkOutFile);
            fprintf(msg);
            fprintf(logFID, msg);

            dataStruct = repmat(emptyRec, 0, 1); % clear memory
        end
    end

    % Save remaining records
    if ~isempty(dataStruct)
        chunkCounter = chunkCounter + 1;
        chunkOutFile = fullfile(outputFolder, sprintf('%s_part%d.mat', baseName, chunkCounter));
        partialDataStruct = dataStruct; %#ok<NASGU>
        save(chunkOutFile, 'partialDataStruct', '-v7.3');

        msg = sprintf('  -> saved final chunk %d (%d records)\n', ...
            chunkCounter, numel(dataStruct));
        fprintf(msg);
        fprintf(logFID, msg);
    end

    msg = sprintf('  Total records parsed: %d\n\n', recCounter);
    fprintf(msg);
    fprintf(logFID, msg);
end


function val = safeField(rec, fieldName)
    if isfield(rec, fieldName)
        val = rec.(fieldName);
    else
        val = [];
    end
end

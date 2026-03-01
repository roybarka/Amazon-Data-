% MATLAB script to read JSONL file, process data, and save in chunks
clear

% File paths
[filename, filepath] = uigetfile('*.jsonl', 'Select the JSONL file');

% Check if the user canceled the selection
if isequal(filename, 0)
    error('No file selected. Please select a valid JSONL file.');
else
    inputFilePath = fullfile(filepath, filename); % Combine the folder and file name
    fprintf('Selected file: %s\n', inputFilePath);
end

outputFileName = 'F:\Roy\Amazon_data\Mat_files\Home_and_Kitchen\Home_and_Kitchen'; % Base name for output .mat files
lookupTableFileName = 'F:\Roy\Amazon_data\LookUp Table\Home_and_KitchenLookUpTable.mat'; % File name for the lookup table

% Parameters
chunkSize = 10000; % Number of items per chunk

% Initialize variables
asinSet = containers.Map('KeyType', 'char', 'ValueType', 'logical');
asinIndexMap = containers.Map('KeyType', 'char', 'ValueType', 'double');
lookupTable = containers.Map('KeyType', 'double', 'ValueType', 'char');
counter = 0;
uniqueIDCounter = 0;
ratingAdded = true;
fid = fopen(inputFilePath, 'r');


% % for starting with saved workspace , put in comment all the above code and use this:
% counter = counter -1 ;
% ratingAdded = true;
% fid = fopen(inputFilePath, 'r');


% Loop through the file and process in chunks
while ratingAdded
    dataStruct = struct('ratings', {}, 'titles', {}, 'texts', {}, 'timestamps', {}, 'parent_asins', {}, 'user_ids', {}, 'verified_purchases', {}, 'helpful_votes', {}, 'images', {}, 'asins', {}, 'unique_ids', {});
    asinIndexMap = containers.Map('KeyType', 'char', 'ValueType', 'double');
    ratingAdded = false;
    counter = counter + 1;

    % Rewind the file to the beginning
    fseek(fid, 0, 'bof');
    iteration_counter = 0;
    
    % Read lines from the file
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
            dataStruct(idx).timestamps(end+1) = data.timestamp / 1000; % Convert to seconds
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
            lookupTable(uniqueIDCounter) = asin; % Add to lookup table
        end
    end
    
    % Sort each asin by timestamp
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
    
    if ~isempty(dataStruct)  % Ensure there is data to save
        startIdx = (counter - 1) * chunkSize + 1;
        endIdx = startIdx + chunkSize - 1;
        saveFileName = sprintf('%s%d_%d.mat', outputFileName, startIdx, endIdx);
        fprintf('starting to save %s at %s. num of revies is: %d\n', saveFileName, datetime("now"), iteration_counter);
        save(saveFileName, 'dataStruct', '-v7.3');
        fprintf('Saved %s at %s.\n', saveFileName, datetime("now"));
    end
end

% Save the lookup table
lookupTableKeys = keys(lookupTable);
lookupTableValues = values(lookupTable);
lookupTableStruct = struct('unique_id', lookupTableKeys, 'asin', lookupTableValues);
save(lookupTableFileName, 'lookupTableStruct', '-v7.3');
fprintf('Saved lookup table %s\n', lookupTableFileName);

% Close the file
fclose(fid);

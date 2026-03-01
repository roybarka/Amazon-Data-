% Restore_All_LookupTables.m
% This script iterates over all category folders in 'Mat_Files',
% extracts unique_ids and asins, and generates a Lookup Table for each category.

clear; clc;

% =========================================================================
% 1. Configuration
% =========================================================================

% Define Root Directories
matFilesRoot = 'G:\AmazonData\Mat_Files';
outputDir = 'G:\AmazonData\LookUp Table';

% Create output directory if it doesn't exist
if ~exist(outputDir, 'dir')
    mkdir(outputDir);
end

% Get list of all subdirectories (Categories) in Mat_Files
dirs = dir(matFilesRoot);
dirFlags = [dirs.isdir];
subDirs = dirs(dirFlags);
% Filter out '.' and '..'
subDirs = subDirs(~ismember({subDirs.name}, {'.', '..'}));

fprintf('Found %d category folders to process.\n', length(subDirs));

% =========================================================================
% 2. Process Each Category
% =========================================================================

for k = 1:length(subDirs)
    categoryName = subDirs(k).name;
    folderPath = fullfile(matFilesRoot, categoryName);
    
    % Define Output Filename
    outputFileName = [categoryName 'LookUpTable.mat'];
    outputFullPath = fullfile(outputDir, outputFileName);
    
    % Optional: Skip if already exists
    if exist(outputFullPath, 'file')
       fprintf('Skipping %s (File already exists)\n', categoryName);
       continue;
    end
    
    fprintf('\nProcessing Category (%d/%d): %s\n', k, length(subDirs), categoryName);
    
    % ---------------------------------------------------------------------
    % A. Get and Sort Files (User's Logic)
    % ---------------------------------------------------------------------
    files = dir(fullfile(folderPath, '*.mat'));
    fileNames = {files.name};
    
    if isempty(fileNames)
        fprintf('  No .mat files found in %s. Skipping.\n', categoryName);
        continue;
    end
    
    % Extract first number from each file name for sorting
    firstNums = zeros(1, numel(fileNames));
    for f = 1:numel(fileNames)
        tokens = regexp(fileNames{f}, '\d+', 'match'); 
        if ~isempty(tokens)
            firstNums(f) = str2double(tokens{1});
        else
            firstNums(f) = 0; % Fallback
        end
    end
    
    % Sort files by first number
    [~, idx] = sort(firstNums);
    fileNames = fileNames(idx);
    
    % ---------------------------------------------------------------------
    % B. Extract Data (Vectorized for Speed)
    % ---------------------------------------------------------------------
    
    % Initialize accumulators
    % We use cell arrays for ASINs and a vector for IDs for better performance
    all_unique_ids = [];
    all_asins = {};
    
    for i = 1:numel(fileNames)
        filePath = fullfile(folderPath, fileNames{i});
        
        try
            % Load data
            loadedData = load(filePath);
            varNames = fieldnames(loadedData);
            currentStruct = loadedData.(varNames{1}); % Assuming struct array
            
            % Vectorized Extraction (Replaces the slow inner loop)
            % Extract all unique_ids into a vector
            if isfield(currentStruct, 'unique_ids')
                batch_ids = [currentStruct.unique_ids];
                all_unique_ids = [all_unique_ids, batch_ids]; %#ok<AGROW>
            end
            
            % Extract all asins into a cell array
            if isfield(currentStruct, 'asins')
                batch_asins = {currentStruct.asins};
                all_asins = [all_asins, batch_asins]; %#ok<AGROW>
            end
            
            if mod(i, 5) == 0 || i == numel(fileNames)
                fprintf('  Processed file %d/%d: %s\n', i, numel(fileNames), fileNames{i});
            end
            
        catch ME
            fprintf('  Error reading file %s: %s\n', fileNames{i}, ME.message);
        end
    end
    
    % ---------------------------------------------------------------------
    % C. Construct and Save
    % ---------------------------------------------------------------------
    
    if isempty(all_unique_ids)
        fprintf('  No data found for %s. Skipping save.\n', categoryName);
        continue;
    end
    
    fprintf('  Constructing final struct table...\n');
    
    % Convert back to struct array format as requested: struct('unique_id', {}, 'asin', {})
    % Note: cell2struct is much faster than loop assignment
    
    % Ensure dimensions match
    numItems = length(all_unique_ids);
    
    % Create the final struct array
    % 1. Convert IDs to cell to match struct creation syntax
    idsCell = num2cell(all_unique_ids);
    
    % 2. Create struct
    lookupTableStruct = struct('unique_id', idsCell, 'asin', all_asins);
    
    % Save
    fprintf('  Saving to %s ...\n', outputFullPath);
    save(outputFullPath, 'lookupTableStruct', '-v7.3');
    fprintf('  Saved successfully.\n');
    
    % Clear large variables to free RAM for next category
    clear lookupTableStruct all_unique_ids all_asins idsCell;
end

fprintf('\nAll categories processed.\n');
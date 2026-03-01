% check_duplicate_parent_asin.m
% סקריפט שסורק קובץ meta_data.jsonl ובודק אם יש רשומות עם אותו parent_asin

clear; clc;

% --- בחירת קובץ meta_*.jsonl ---
[filename, filepath] = uigetfile({'meta_*.jsonl;*.jsonl','JSON Lines (*.jsonl)'}, ...
    'Select a META JSONL file','..\Meta_Data');
if isequal(filename,0)
    error('No file selected.');
end
inputFilePath = fullfile(filepath, filename);
fprintf('Selected file: %s\n', inputFilePath);

% --- קריאת כל השורות ---
fid = fopen(inputFilePath,'r','n','UTF-8');
if fid == -1
    error('Failed to open file.');
end
cleanupObj = onCleanup(@() fclose(fid));

parent_asins = {};

lineNo = 0;
while ~feof(fid)
    line = fgetl(fid);
    if ~ischar(line), break; end
    lineNo = lineNo + 1;
    try
        rec = jsondecode(line);
        if isfield(rec,'parent_asin') && ~isempty(rec.parent_asin)
            parent_asins{end+1} = rec.parent_asin; %#ok<AGROW>
        else
            parent_asins{end+1} = ''; %#ok<AGROW>
        end
    catch
        warning('Failed to parse line %d', lineNo);
    end
end

fprintf('Total records read: %d\n', numel(parent_asins));

% --- בדיקת כפילויות ---
[uniqueParents, ~, idx] = unique(parent_asins);
counts = accumarray(idx, 1);

dupMask = counts > 1;
duplicateParents = uniqueParents(dupMask);

fprintf('נמצאו %d parent_asin שמופיעים יותר מפעם אחת\n', numel(duplicateParents));

% הצג כמה דוגמאות
if ~isempty(duplicateParents)
    disp('דוגמאות לערכים כפולים:');
    disp(duplicateParents(1:min(10,end)));
end

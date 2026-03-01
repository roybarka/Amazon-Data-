% Amazon_meta_preview.m
% Preview structure of Amazon Reviews'23 item metadata JSONL files
% (reads a few lines and prints fields/types/examples)

clear; clc;

% --- Parameters ---
previewCount = 5;   % how many items to preview
maxArrayPreview = 5; % cap when printing array contents

% --- Choose file ---
[filename, filepath] = uigetfile({'meta_*.jsonl;*.jsonl','JSON Lines (*.jsonl)'}, ...
    'Select a META JSONL file (e.g., meta_All_Beauty.jsonl)');
if isequal(filename,0)
    error('No file selected.');
end
inputFilePath = fullfile(filepath, filename);
fprintf('Selected file: %s\n\n', inputFilePath);

% --- Open file ---
fid = fopen(inputFilePath,'r','n','UTF-8');
if fid == -1
    error('Failed to open file.');
end

cleanupObj = onCleanup(@() fclose(fid));

samples = {};
unionFields = string.empty(1,0);
lineNo = 0;

fprintf('=== Previewing first %d records ===\n\n', previewCount);

while numel(samples) < previewCount
    line = fgetl(fid);
    if ~ischar(line), break; end
    lineNo = lineNo + 1;
    line = strtrim(line);
    if isempty(line), continue; end
    try
        rec = jsondecode(line);
        samples{end+1} = rec; %#ok<AGROW>

        % update union of fields
        fns = string(fieldnames(rec));
        unionFields = unique([unionFields, fns]); %#ok<AGROW>

        % pretty print this record
        fprintf('--- Record %d (line %d) ---\n', numel(samples), lineNo);
        ppKeyVal(rec, 'parent_asin');
        ppKeyVal(rec, 'title');
        ppKeyVal(rec, 'main_category');
        ppKeyVal(rec, 'average_rating');
        ppKeyVal(rec, 'rating_number');
        ppKeyVal(rec, 'price');
        ppArraySummary(rec, 'features', maxArrayPreview);
        ppArraySummary(rec, 'description', maxArrayPreview);
        ppArraySummary(rec, 'categories', maxArrayPreview);

        % images: array of structs with fields (thumb/large/variant/hi_res)
        if isfield(rec,'images') && ~isempty(rec.images)
            imgs = rec.images;
            nImgs = numel(imgs);
            fprintf('images: [%d items]', nImgs);
            toShow = min(nImgs, maxArrayPreview);
            if isstruct(imgs)
                variants = strings(1,toShow);
                hasHiRes = false(1,toShow);
                for k = 1:toShow
                    v = '';
                    if isfield(imgs(k),'variant') && ~isempty(imgs(k).variant), v = imgs(k).variant; end
                    variants(k) = string(v);
                    hasHiRes(k) = isfield(imgs(k),'hi_res') && ~isempty(imgs(k).hi_res);
                end
                fprintf(' | first %d variants: %s | hi_res flags: %s\n', ...
                    toShow, strjoin(variants, ', '), mat2str(hasHiRes));
            else
                fprintf('\n');
            end
        else
            fprintf('images: []\n');
        end

        % videos (just count)
        if isfield(rec,'videos') && ~isempty(rec.videos)
            fprintf('videos: [%d items]\n', numel(rec.videos));
        else
            fprintf('videos: []\n');
        end

        % details: map of string->string (print keys)
        if isfield(rec,'details') && ~isempty(rec.details) && isstruct(rec.details)
            dKeys = string(fieldnames(rec.details));
            toShow = min(numel(dKeys), maxArrayPreview);
            fprintf('details: %d keys | first keys: %s\n', numel(dKeys), strjoin(dKeys(1:toShow), ', '));
        else
            fprintf('details: {}\n');
        end

        ppKeyVal(rec, 'store');
        ppKeyVal(rec, 'bought_together');

        fprintf('\n');
    catch ME
        warning('Failed to parse line %d: %s', lineNo, ME.message);
    end
end

% --- Union-of-fields summary ---
fprintf('=== Union of top-level fields seen in first %d records ===\n', numel(samples));
unionFields = sort(unionFields);
disp(unionFields');

% --- Quick type snapshot per field (based on first record that has it) ---
fprintf('\n=== Example type per field (first non-empty occurrence) ===\n');
for f = unionFields
    val = [];
    for i = 1:numel(samples)
        if isfield(samples{i}, f) && ~isempty(samples{i}.(f))
            val = samples{i}.(f);
            break;
        end
    end
    if isempty(val)
        tstr = '[]';
        ex = '';
    else
        tstr = class(val);
        ex = summarizeValue(val, maxArrayPreview);
    end
    fprintf('%s : %s %s\n', f, tstr, ex);
end

fprintf('\nDone.\n');

% ========= helpers =========
function ppKeyVal(rec, key)
    if isfield(rec, key)
        val = rec.(key);
        if ischar(val) || (isstring(val) && isscalar(val))
            vstr = string(val);
        elseif isnumeric(val) && isscalar(val)
            vstr = string(val);
        elseif isempty(val)
            vstr = "[]";
        else
            vstr = sprintf('[%s]', class(val));
        end
        fprintf('%s: %s\n', key, vstr);
    else
        fprintf('%s: <missing>\n', key);
    end
end

function ppArraySummary(rec, key, cap)
    if isfield(rec,key) && ~isempty(rec.(key))
        v = rec.(key);
        if iscell(v) || isstring(v)
            n = numel(v);
            toShow = min(n, cap);
            try
                headVals = string(v(1:toShow));
                headVals = arrayfun(@(s) clipstr(s,80), headVals, 'UniformOutput', false);
                fprintf('%s: [%d items] | first %d: %s\n', key, n, toShow, strjoin(headVals, ' | '));
            catch
                fprintf('%s: [%d items]\n', key, n);
            end
        else
            fprintf('%s: [%s]\n', key, class(v));
        end
    else
        fprintf('%s: []\n', key);
    end
end

function s = summarizeValue(v, cap)
    if ischar(v) || (isstring(v) && isscalar(v))
        s = sprintf('| e.g., "%s"', clipstr(string(v), 80));
    elseif isnumeric(v) && isscalar(v)
        s = sprintf('| e.g., %g', v);
    elseif iscell(v) || isstring(v)
        n = numel(v); s = sprintf('| array(%d)', n);
        if n>0
            toShow = min(n, cap);
            try
                headVals = string(v(1:toShow));
                headVals = arrayfun(@(x) clipstr(x,60), headVals, 'UniformOutput', false);
                s = sprintf('%s | first %d: %s', s, toShow, strjoin(headVals, ', '));
            catch
            end
        end
    elseif isstruct(v)
        s = '| struct';
        try
            fns = fieldnames(v);
            toShow = min(numel(fns), cap);
            s = sprintf('%s (fields: %s%s)', s, strjoin(fns(1:toShow), ', '), ...
                ternary(numel(fns)>toShow, ', ...', ''));
        catch
        end
    else
        s = '';
    end
end

function out = clipstr(strIn, maxlen)
    s = char(strIn);
    if numel(s) > maxlen
        out = string([s(1:maxlen-3) '...']);
    else
        out = string(s);
    end
end

function t = ternary(cond, a, b)
    if cond, t = a; else, t = b; end
end

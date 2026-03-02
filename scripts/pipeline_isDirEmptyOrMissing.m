function [isEmpty, fileCount] = pipeline_isDirEmptyOrMissing(dirPath, filePattern)
% PIPELINE_ISDIREMPTYORMISSING  True if directory does not exist or has no matching files.
%
% Usage:
%   [isEmpty, fileCount] = pipeline_isDirEmptyOrMissing(dirPath);
%   [isEmpty, fileCount] = pipeline_isDirEmptyOrMissing(dirPath, '*.mat');
%
% If filePattern is omitted, defaults to '*.*' (any file). Returns fileCount of matching files.

if nargin < 2
    filePattern = '*.*';
end

if ~exist(dirPath, 'dir')
    isEmpty = true;
    fileCount = 0;
    return;
end

list = dir(fullfile(dirPath, filePattern));
% Exclude . and ..
list = list(~ismember({list.name}, {'.', '..'}));
fileCount = numel(list);
isEmpty = (fileCount == 0);
end

% RUN_PIPELINE  Orchestrator: scan external data drive, detect missing pipeline outputs, run steps and resume.
%
% Usage:
%   Run_Pipeline();
%   Run_Pipeline(pipelineCfg);
%
% pipelineCfg (optional struct):
%   .dataRoot       - root containing category folders (default: 'F:\Amazon_data Part 2')
%   .scriptsDir     - path to scripts folder (default: fileparts(which('Run_Pipeline')))
%   .logFile        - path to execution log (default: <dataRoot>/pipeline_execution_log.txt)
%   .stateFile      - path to state file for resume (default: <dataRoot>/pipeline_state.mat)
%   .jsonlByCategory - struct: field names = category names, values = path to .jsonl (optional)
%   .metaJsonlByCategory - struct: field names = category names, values = path to folder with meta_*.jsonl (optional)
%   .metaJsonlRoot - single folder containing all meta_*.jsonl (e.g. F:\Amazon_data json\Meta_Data). Used for every category when metaJsonlByCategory is not set; script processes only meta_<CategoryName>.jsonl per category.

function Run_Pipeline(pipelineCfg)

if nargin < 1
    pipelineCfg = struct();
end

dataRoot = getCfg(pipelineCfg, 'dataRoot', 'F:\Amazon_data Part 2');
scriptsDir = getCfg(pipelineCfg, 'scriptsDir', fileparts(which('Run_Pipeline')));
if isempty(scriptsDir)
    scriptsDir = pwd;
end
logFile = getCfg(pipelineCfg, 'logFile', fullfile(dataRoot, 'pipeline_execution_log.txt'));
stateFile = getCfg(pipelineCfg, 'stateFile', fullfile(dataRoot, 'pipeline_state.mat'));

% Ensure scripts and helper are on path
addpath(scriptsDir);
addpath(fullfile(scriptsDir, 'helper'));

if ~exist(dataRoot, 'dir')
    error('Data root does not exist: %s', dataRoot);
end

% Load state for resume
state = loadState(stateFile);

% Open log (append)
logFID = fopen(logFile, 'a');
if logFID == -1
    warning('Could not open log file: %s. Logging to console only.', logFile);
    logFID = [];
end
cleanupLog = onCleanup(@() closeLog(logFID));

writeLog(logFID, logFile, sprintf('=== Pipeline run started at %s ===', datestr(now)));
writeLog(logFID, logFile, sprintf('Data root: %s', dataRoot));
writeLog(logFID, logFile, sprintf('State file: %s', stateFile));

% List category folders (directories under dataRoot, excluding . and ..)
dirs = dir(dataRoot);
dirFlags = [dirs.isdir];
subDirs = dirs(dirFlags);
subDirs = subDirs(~ismember({subDirs.name}, {'.', '..'}));
categoryNames = {subDirs.name};

if isempty(categoryNames)
    writeLog(logFID, logFile, 'No category folders found. Exiting.');
    return;
end

writeLog(logFID, logFile, sprintf('Found %d categories: %s', numel(categoryNames), strjoin(categoryNames, ', ')));

for c = 1:numel(categoryNames)
    categoryName = categoryNames{c};
    categoryDir = fullfile(dataRoot, categoryName);
    if ~isdir(categoryDir)
        continue;
    end

    writeLog(logFID, logFile, sprintf('--- Category %d/%d: %s ---', c, numel(categoryNames), categoryName));

    paths = pipeline_getCategoryPaths(dataRoot, categoryName);
    [stepsToRun, ~] = pipeline_getStepsForMissingDirs(paths, categoryName);

    if isempty(stepsToRun)
        writeLog(logFID, logFile, sprintf('  [%s] No missing steps. Skip.', categoryName));
        continue;
    end

    for s = 1:numel(stepsToRun)
        step = stepsToRun{s};
        stepName = step.name;

        % Resume: skip if already completed
        if isStepCompleted(state, categoryName, stepName)
            writeLog(logFID, logFile, sprintf('  [%s] %s already completed (resume). Skip.', categoryName, stepName));
            continue;
        end

        % Skip steps that require input we do not have
        if ~isempty(step.requiresInput)
            if strcmp(step.requiresInput, 'jsonlFilePath')
                jsonlPath = getCategoryInput(pipelineCfg, 'jsonlByCategory', categoryName);
                if isempty(jsonlPath) || ~exist(jsonlPath, 'file')
                    writeLog(logFID, logFile, sprintf('  [%s] %s skipped (no jsonlFilePath).', categoryName, stepName));
                    continue;
                end
            elseif strcmp(step.requiresInput, 'metaJsonlFolder')
                metaFolder = getCategoryInput(pipelineCfg, 'metaJsonlByCategory', categoryName);
                if isempty(metaFolder)
                    metaFolder = getCfg(pipelineCfg, 'metaJsonlRoot', '');
                end
                if isempty(metaFolder) || ~exist(metaFolder, 'dir')
                    writeLog(logFID, logFile, sprintf('  [%s] %s skipped (no metaJsonlFolder or metaJsonlRoot).', categoryName, stepName));
                    continue;
                end
            end
        end

        writeLog(logFID, logFile, sprintf('  [%s] Running %s...', categoryName, stepName));

        cfg = struct('categoryRoot', dataRoot, 'categoryName', categoryName);
        if ~isempty(step.requiresInput)
            if strcmp(step.requiresInput, 'jsonlFilePath')
                cfg.jsonlFilePath = getCategoryInput(pipelineCfg, 'jsonlByCategory', categoryName);
            elseif strcmp(step.requiresInput, 'metaJsonlFolder')
                cfg.metaJsonlFolder = getCategoryInput(pipelineCfg, 'metaJsonlByCategory', categoryName);
                if isempty(cfg.metaJsonlFolder)
                    cfg.metaJsonlFolder = getCfg(pipelineCfg, 'metaJsonlRoot', '');
                end
            end
        end

        try
            ok = runStep(stepName, cfg, scriptsDir);
            if ok
                state = markStepCompleted(state, categoryName, stepName);
                saveState(stateFile, state);
                writeLog(logFID, logFile, sprintf('  [%s] %s OK.', categoryName, stepName));
            else
                writeLog(logFID, logFile, sprintf('  [%s] %s FAILED (returned false).', categoryName, stepName));
                break; % move to next category
            end
        catch ME
            writeLog(logFID, logFile, sprintf('  [%s] %s ERROR: %s', categoryName, stepName, ME.message));
            break; % move to next category
        end
    end
end

writeLog(logFID, logFile, sprintf('=== Pipeline run finished at %s ===', datestr(now)));
end


function v = getCfg(cfg, field, default)
    if isstruct(cfg) && isfield(cfg, field)
        v = cfg.(field);
    else
        v = default;
    end
    if isempty(v) && nargin >= 3
        v = default;
    end
end


function pathVal = getCategoryInput(pipelineCfg, fieldName, categoryName)
    pathVal = [];
    if ~isfield(pipelineCfg, fieldName)
        return;
    end
    s = pipelineCfg.(fieldName);
    if isstruct(s) && isfield(s, categoryName)
        pathVal = s.(categoryName);
    end
end


function ok = runStep(stepName, cfg, scriptsDir)
    ok = false;
    switch stepName
        case 'Amazon_data_script'
            ok = Amazon_data_script(cfg);
        case 'Restore_LookupTable'
            ok = Restore_LookupTable(cfg);
        case 'reduced_form_script_10000'
            ok = reduced_form_script_10000(cfg);
        case 'reduced_form2matrix'
            ok = reduced_form2matrix(cfg);
        case 'Matrix2csv'
            ok = Matrix2csv(cfg);
        case 'Amazon_meta_data_script'
            ok = Amazon_meta_data_script(cfg);
        case 'Enrich_Lookup_Table'
            ok = Enrich_Lookup_Table(cfg);
        case 'generate_category_sparse_matrix'
            ok = generate_category_sparse_matrix(cfg);
        otherwise
            error('Unknown step: %s', stepName);
    end
end


function state = loadState(stateFile)
    state = struct('completedSteps', cell(0, 2));
    if exist(stateFile, 'file')
        try
            loaded = load(stateFile, 'state');
            state = loaded.state;
            % Ensure scalar struct so state.completedSteps is a single value (avoids comma-separated list)
            if isempty(state) || numel(state) ~= 1
                state = struct('completedSteps', cell(0, 2));
            elseif ~isfield(state, 'completedSteps')
                state.completedSteps = cell(0, 2);
            end
        catch
            state = struct('completedSteps', cell(0, 2));
        end
    end
end


function saveState(stateFile, state)
    state.lastRun = datestr(now);
    try
        save(stateFile, 'state', '-v7.3');
    catch ME
        warning('Could not save state file: %s', ME.message);
    end
end


function yes = isStepCompleted(state, categoryName, stepName)
    yes = false;
    if isempty(state) || numel(state) ~= 1
        return;
    end
    if ~isfield(state, 'completedSteps')
        return;
    end
    steps = state.completedSteps;
    if isempty(steps)
        return;
    end
    for i = 1:size(steps, 1)
        if strcmp(steps{i, 1}, categoryName) && strcmp(steps{i, 2}, stepName)
            yes = true;
            return;
        end
    end
end


function state = markStepCompleted(state, categoryName, stepName)
    if ~isfield(state, 'completedSteps')
        state.completedSteps = cell(0, 2);
    end
    state.completedSteps(end+1, 1) = {categoryName};
    state.completedSteps(end, 2) = {stepName};
end


function writeLog(logFID, logFile, msg)
    fprintf('%s\n', msg);
    if ~isempty(logFID) && logFID > 0
        try
            fprintf(logFID, '%s\n', msg);
        catch
        end
    end
end


function closeLog(logFID)
    if ~isempty(logFID) && logFID > 0
        try
            fclose(logFID);
        catch
        end
    end
end

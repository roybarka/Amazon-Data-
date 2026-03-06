function [stepsToRun, details] = pipeline_getStepsForMissingDirs(paths, categoryName)
% PIPELINE_GETSTEPSFORMISSINGDIRS  Determine which pipeline steps to run based on empty/missing dirs.
% Returns steps in execution order. Each step is a struct with .name and .fillsDir.
%
% Pipeline order and which script fills which directory:
%   1. Amazon_data_script     -> Mat_Files, LookUp_Table
%   2. reduced_form_script    -> Processed_MAT_files  (reads Mat_Files)
%   3. reduced_form2matrix    -> Matrix, Matrix_Lookup  (reads Processed_MAT_files)
%   4. Matrix2csv             -> Matrix_CSV  (reads Matrix)
%   5. Amazon_meta_data_script -> Meta_Data_Mat_files, Meta_Data_LookUp_Table (optional)
%   Restore_LookupTable      -> LookUp_Table only (if Mat_Files exist but LookUp missing)

stepsToRun = {};
details = struct();

[matEmpty, matCount]    = pipeline_isDirEmptyOrMissing(paths.Mat_Files, '*.mat');
[procEmpty, procCount]  = pipeline_isDirEmptyOrMissing(paths.Processed_MAT_files, '*.mat');
[matrixEmpty, matrixCount] = pipeline_isDirEmptyOrMissing(paths.Matrix, '*.mat');
[csvEmpty, csvCount]    = pipeline_isDirEmptyOrMissing(paths.Matrix_CSV, '*.csv');
lookupExists = exist(paths.lookupTableFile, 'file');
[metaMatEmpty, ~]       = pipeline_isDirEmptyOrMissing(paths.Meta_Data_Mat_files, '*.mat');

details.Mat_Files_empty = matEmpty;
details.Mat_Files_count = matCount;
details.Processed_MAT_files_empty = procEmpty;
details.Matrix_empty = matrixEmpty;
details.Matrix_CSV_empty = csvEmpty;
details.LookUp_Table_exists = lookupExists;
details.Meta_Data_Mat_files_empty = metaMatEmpty;

% Step 1: Raw data or Lookup. If Mat_Files empty -> need Amazon_data_script (requires JSONL).
% If Mat_Files exist but LookUp_Table missing -> use Restore_LookupTable (no JSONL).
if matEmpty
    stepsToRun{end+1} = struct('name', 'Amazon_data_script', 'fillsDir', 'Mat_Files', 'requiresInput', 'jsonlFilePath');
elseif ~lookupExists
    stepsToRun{end+1} = struct('name', 'Restore_LookupTable', 'fillsDir', 'LookUp_Table', 'requiresInput', '');
end

% Step 2: Reduced form
if ~matEmpty && procEmpty
    stepsToRun{end+1} = struct('name', 'reduced_form_script_10000', 'fillsDir', 'Processed_MAT_files', 'requiresInput', '');
end

% Step 3: Matrix + Matrix_Lookup (add if Matrix empty and we have or will have Processed)
willHaveProcessed = ~procEmpty || (~matEmpty && procEmpty);
if matrixEmpty && willHaveProcessed
    stepsToRun{end+1} = struct('name', 'reduced_form2matrix', 'fillsDir', 'Matrix', 'requiresInput', '');
end

% Step 4: Matrix -> CSV (add if Matrix_CSV empty and we have or will have Matrix)
willHaveMatrix = ~matrixEmpty || (matrixEmpty && willHaveProcessed);
if csvEmpty && willHaveMatrix
    stepsToRun{end+1} = struct('name', 'Matrix2csv', 'fillsDir', 'Matrix_CSV', 'requiresInput', '');
end

% Step 5: Meta (optional; only if we want to fill meta dirs - orchestrator can skip if no meta JSONL)
if metaMatEmpty
    stepsToRun{end+1} = struct('name', 'Amazon_meta_data_script', 'fillsDir', 'Meta_Data_Mat_files', 'requiresInput', 'metaJsonlFolder');
end

% Step 6: Enriched lookup (requires base LookUp + Category Lookup + Meta_Data_Mat_files; add if we have or will have both)
enrichedExists = exist(paths.lookupTableEnrichedFile, 'file');
categoryLookupExists = exist(paths.metaCategoryLookupFile, 'file');
metaPartFiles = dir(fullfile(paths.Meta_Data_Mat_files, ['meta_' categoryName '_part*.mat']));
metaPartsExist = ~isempty(metaPartFiles);
willHaveMetaAndCatLookup = (categoryLookupExists && metaPartsExist) || metaMatEmpty;
if lookupExists && ~enrichedExists && willHaveMetaAndCatLookup
    stepsToRun{end+1} = struct('name', 'Enrich_Lookup_Table', 'fillsDir', 'LookUp_Table', 'requiresInput', '');
end

% Step 7: Sparse matrix (requires Enriched + Category Lookup; add if Enriched exists or will be created this run)
sparseMatrixExists = exist(paths.sparseMatrixFile, 'file');
enrichedWillExist = enrichedExists || (lookupExists && ~enrichedExists && willHaveMetaAndCatLookup);
willHaveCategoryLookup = categoryLookupExists || metaMatEmpty;
if ~sparseMatrixExists && enrichedWillExist && willHaveCategoryLookup
    stepsToRun{end+1} = struct('name', 'generate_category_sparse_matrix', 'fillsDir', 'Asin_Category_Matrix', 'requiresInput', '');
end
end

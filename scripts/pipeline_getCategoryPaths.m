function paths = pipeline_getCategoryPaths(categoryRoot, categoryName)
% PIPELINE_GETCATEGORYPATHS  Return struct of paths for Category -> File Type layout.
% Paths are: categoryRoot/categoryName/Mat_Files, ... (no nested category-by-type).
%
% Usage:
%   paths = pipeline_getCategoryPaths('F:\Amazon_data Part 2', 'Digital_Music');
%   paths.Mat_Files, paths.Processed_MAT_files, paths.Matrix, etc.

categoryDir = fullfile(categoryRoot, categoryName);

paths = struct();
paths.categoryDir       = categoryDir;
paths.Mat_Files          = fullfile(categoryDir, 'Mat_Files');
paths.Processed_MAT_files = fullfile(categoryDir, 'Processed_MAT_files');
paths.Matrix              = fullfile(categoryDir, 'Matrix');
paths.Matrix_CSV          = fullfile(categoryDir, 'Matrix_CSV');
paths.LookUp_Table         = fullfile(categoryDir, 'LookUp_Table');
paths.Matrix_Lookup        = fullfile(categoryDir, 'Matrix_Lookup');
paths.Meta_Data_Mat_files = fullfile(categoryDir, 'Meta_Data_Mat_files');
paths.Meta_Data_LookUp_Table = fullfile(categoryDir, 'Meta_Data_LookUp_Table');
paths.Asin_Category_Matrix  = fullfile(categoryDir, 'Asin_Category_Matrix');
paths.TooBigReviews       = fullfile(categoryDir, 'TooBigReviews');

paths.lookupTableFile     = fullfile(paths.LookUp_Table, [categoryName 'LookUpTable.mat']);
paths.lookupTableEnrichedFile = fullfile(paths.LookUp_Table, [categoryName 'LookUpTable_Enriched.mat']);
paths.metaCategoryLookupFile  = fullfile(paths.Meta_Data_LookUp_Table, ['meta_' categoryName '_Category_Lookup.mat']);
paths.sparseMatrixFile     = fullfile(paths.Asin_Category_Matrix, [categoryName 'LookUpTable_Enriched_SparseMatrix.mat']);
paths.matFilesBaseName    = fullfile(paths.Mat_Files, categoryName);  % base for chunk .mat names
end

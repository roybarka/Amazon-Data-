function ok = reduced_form_script_10000(cfg)
% REDUCED_FORM_SCRIPT_10000  Process raw .mat chunks into reduced-form .mat (time windows, etc.).
% Fully automated: no GUI. Uses config with Category -> File Type paths.
%
% cfg.categoryRoot, cfg.categoryName. Optional: cfg.chunkSize (default 10000).
% Reads: Mat_Files/*.mat. Writes: Processed_MAT_files/*_processed_*.mat, optional TooBigReviews.

ok = false;
if nargin < 1 || ~isstruct(cfg)
    error('reduced_form_script_10000 requires config struct cfg with categoryRoot, categoryName.');
end

paths = pipeline_getCategoryPaths(cfg.categoryRoot, cfg.categoryName);
inputDir = paths.Mat_Files;
outputDir = paths.Processed_MAT_files;
largeReviewDir = paths.TooBigReviews;
categoryName = cfg.categoryName;

if ~exist(inputDir, 'dir')
    error('Input directory does not exist: %s', inputDir);
end
if ~exist(outputDir, 'dir'), mkdir(outputDir); end
if ~exist(largeReviewDir, 'dir'), mkdir(largeReviewDir); end

chunkSize = 10000;
if isfield(cfg, 'chunkSize') && isnumeric(cfg.chunkSize) && cfg.chunkSize > 0
    chunkSize = cfg.chunkSize;
end

matFiles = dir(fullfile(inputDir, '*.mat'));
% Match filenames like CategoryName1_10000.mat (no underscore between name and first number)
pattern = [categoryName '%d_%*d.mat'];
fileNumbers = zeros(1, numel(matFiles));
for f = 1:numel(matFiles)
    n = sscanf(matFiles(f).name, pattern);
    if ~isempty(n)
        fileNumbers(f) = n(1);
    else
        tok = regexp(matFiles(f).name, '\d+', 'match');
        if ~isempty(tok), fileNumbers(f) = str2double(tok{1}); end
    end
end
[~, sortIdx] = sort(fileNumbers);
matFiles = matFiles(sortIdx);

if isempty(matFiles)
    fprintf('No .mat files found in %s. Skipping.\n', inputDir);
    ok = true;
    return;
end

emptyStruct = struct('unique_ids', {}, 'asins', {}, 'parent_asins', {}, 'ratings', {}, 'user_ids', {}, 'verified_purchases', {}, 'helpful_votes', {}, 'timestamps', {}, 'images', {}, ...
    'days_since_first_review', {}, 'reviews_delta_time', {}, 'total_num_prev_reviews', {}, 'total_num_prev_reviews_for_days', {}, ...
    'num_prev_stars_for_0days', {}, 'num_prev_stars_for_1days', {}, 'num_prev_stars_for_2days', {}, 'num_prev_stars_for_3days', {}, 'num_prev_stars_for_4days', {}, 'num_prev_stars_for_5days', {}, 'num_prev_stars_for_6days', {}, 'num_prev_stars_for_7days', {}, ...
    'delta_time_OutOfWindow', {}, 'text_size', {}, 'is_last_review', {}, 'is_last_review_week', {}, 'num_simultaneous_reviews', {});
combinedDataStruct = emptyStruct;
processedFilesCounter = 0;

for i = 1:length(matFiles)
    inputFile = fullfile(inputDir, matFiles(i).name);
    fprintf('Processing %s\n', inputFile);

    loadedData = load(inputFile);
    originalDataStruct = loadedData.dataStruct;

    for j = 1:length(originalDataStruct)
        try
            combinedDataStruct(end + 1).unique_ids = originalDataStruct(j).unique_ids;
            index2NoDuplicates = CleanDuplicates(originalDataStruct(j).user_ids);

            combinedDataStruct(end).asins = originalDataStruct(j).asins;
            combinedDataStruct(end).parent_asins = originalDataStruct(j).parent_asins;
            combinedDataStruct(end).ratings = originalDataStruct(j).ratings(index2NoDuplicates);
            combinedDataStruct(end).user_ids = originalDataStruct(j).user_ids(index2NoDuplicates);
            combinedDataStruct(end).verified_purchases = originalDataStruct(j).verified_purchases(index2NoDuplicates);
            combinedDataStruct(end).helpful_votes = originalDataStruct(j).helpful_votes(index2NoDuplicates);
            num_reviews = length(originalDataStruct(j).ratings(index2NoDuplicates));

            combinedDataStruct(end).timestamps = datetime(originalDataStruct(j).timestamps(index2NoDuplicates), 'ConvertFrom', 'posixtime');

            if all(cellfun(@isempty, originalDataStruct(j).images(index2NoDuplicates)))
                combinedDataStruct(end).images = 0;
            else
                combinedDataStruct(end).images = 1;
            end

            first_review_time = combinedDataStruct(end).timestamps(1);
            days_since_first_review = days(combinedDataStruct(end).timestamps - first_review_time);
            combinedDataStruct(end).days_since_first_review = days_since_first_review;

            time_diff_matrix = (combinedDataStruct(end).days_since_first_review - combinedDataStruct(end).days_since_first_review');
            combinedDataStruct(end).reviews_delta_time = [0, diff(combinedDataStruct(end).days_since_first_review)];

            for days_window = 0:7
                num_prev_stars_for_days = zeros(5, num_reviews);
                num_prev_stars_for_days(1,:) = sum((time_diff_matrix > days_window) & (combinedDataStruct(end).ratings' == 1));
                num_prev_stars_for_days(2,:) = sum((time_diff_matrix > days_window) & (combinedDataStruct(end).ratings' == 2));
                num_prev_stars_for_days(3,:) = sum((time_diff_matrix > days_window) & (combinedDataStruct(end).ratings' == 3));
                num_prev_stars_for_days(4,:) = sum((time_diff_matrix > days_window) & (combinedDataStruct(end).ratings' == 4));
                num_prev_stars_for_days(5,:) = sum((time_diff_matrix > days_window) & (combinedDataStruct(end).ratings' == 5));
                combinedDataStruct(end).(sprintf('num_prev_stars_for_%ddays', days_window)) = num_prev_stars_for_days;
            end

            combinedDataStruct(end).total_num_prev_reviews = sum(time_diff_matrix > 0);
            combinedDataStruct(end).total_num_prev_reviews_for_days = [sum(time_diff_matrix>1); sum(time_diff_matrix>2); sum(time_diff_matrix>3); sum(time_diff_matrix>4); sum(time_diff_matrix>5); sum(time_diff_matrix>6); sum(time_diff_matrix>7)];

            delta_time_OutOfWindow = zeros(7, num_reviews);
            for window = 1:7
                out_of_window = time_diff_matrix >= window;
                Index = sum(out_of_window);
                if all(Index == 0)
                    Index = ones(size(Index));
                else
                    Index(1:find(Index > 0, 1) - 1) = 1;
                end
                delta_time_OutOfWindow(window,:) = (combinedDataStruct(end).days_since_first_review - combinedDataStruct(end).days_since_first_review(Index)) .* (combinedDataStruct(end).days_since_first_review > window);
            end
            combinedDataStruct(end).delta_time_OutOfWindow = delta_time_OutOfWindow;

            combinedDataStruct(end).text_size = cellfun(@length, originalDataStruct(j).texts(index2NoDuplicates));
            combinedDataStruct(end).is_last_review = (round(days_since_first_review) == round(days_since_first_review(end)));
            combinedDataStruct(end).is_last_review_week = (days_since_first_review >= (days_since_first_review(end) - 7));
            [~, ~, idx] = unique(round(days_since_first_review));
            counts = accumarray(idx, 1);
            combinedDataStruct(end).num_simultaneous_reviews = counts(idx)';

        catch ME
            if contains(ME.message, 'exceeds maximum array size preference')
                fprintf(2, 'Error: Time difference matrix too large for asin: %s\n', originalDataStruct(j).asins);
                reviewData = originalDataStruct(j);
                asin = originalDataStruct(j).asins;
                largeReviewFile = fullfile(largeReviewDir, [asin, '.mat']);
                save(largeReviewFile, 'reviewData', '-v7.3');
                fprintf('Saved large review to %s\n', largeReviewFile);
            else
                rethrow(ME);
            end
            combinedDataStruct = emptyStruct;
        end
    end

    processedFilesCounter = processedFilesCounter + 1;
    if mod(processedFilesCounter, 1) == 0
        startIdx = (i - 1) * chunkSize + 1;
        endIdx = i * chunkSize;
        outputFileName = fullfile(outputDir, sprintf('%s_processed_%d_%d.mat', categoryName, startIdx, endIdx));
        save(outputFileName, 'combinedDataStruct', '-v7.3');
        fprintf('Saved reduced form to %s\n', outputFileName);
        combinedDataStruct = emptyStruct;
    end
end

if ~isempty(combinedDataStruct)
    startIdx = (processedFilesCounter - mod(processedFilesCounter, 10)) * chunkSize + 1;
    endIdx = processedFilesCounter * chunkSize;
    outputFileName = fullfile(outputDir, sprintf('%s_processed_%d_%d.mat', categoryName, startIdx, endIdx));
    save(outputFileName, 'combinedDataStruct', '-v7.3');
    fprintf('Saved reduced form to %s\n', outputFileName);
end

fprintf('Processing complete.\n');
ok = true;
end

% MATLAB script to iterate over each .mat file, create a new dataStruct, and save it to another .mat file
clear

% Define the directory containing the original .mat files
inputDir = 'F:\Amazon_data\Mat_files\All_Beauty';
outputDir = 'F:\AmazonData_Part1Full\Categories\All_Beauty\Procceced_Mat_Files';
largeReviewDir = 'F:\Roy\Amazon_data\TooBigReviews';

chunkSize = 10000;

% Get a list of all .mat files in the input directory
matFiles = dir(fullfile(inputDir, '*.mat'));

% Extract numerical parts from filenames and sort
fileNumbers = cellfun(@(x) sscanf(x, 'All_Beauty_%d_%*d.mat'), {matFiles.name});
[~, sortIdx] = sort(fileNumbers);
matFiles = matFiles(sortIdx);

% Initialize variables for accumulating data
combinedDataStruct = struct('unique_ids', {}, 'asins', {}, 'parent_asins', {}, 'ratings', {}, 'user_ids', {}, 'verified_purchases', {}, 'helpful_votes', {}, 'timestamps', {}, 'images', {}, ...
    'days_since_first_review', {},'reviews_delta_time', {}, 'total_num_prev_reviews', {},'total_num_prev_reviews_for_days', {}, ...
    'num_prev_stars_for_0days', {},'num_prev_stars_for_1days', {},'num_prev_stars_for_2days', {},'num_prev_stars_for_3days', {},'num_prev_stars_for_4days', {},'num_prev_stars_for_5days', {},'num_prev_stars_for_6days', {},'num_prev_stars_for_7days', {}, ...
    'delta_time_OutOfWindow', {}, 'text_size', {}, 'is_last_review', {}, 'is_last_review_week', {}, 'num_simultaneous_reviews', {});
processedFilesCounter = 0;

% Iterate over each .mat file
for i = 1:length(matFiles)
    inputFile = fullfile(inputDir, matFiles(i).name);
    fprintf('Processing %s\n', inputFile);
     
    % Load the existing dataStruct from the .mat file
    loadedData = load(inputFile);
    originalDataStruct = loadedData.dataStruct;

    % Iterate over the original dataStruct and append to the combinedDataStruct
    for j = 1:length(originalDataStruct)
        try
        % Copy attributes
        combinedDataStruct(end + 1).unique_ids = originalDataStruct(j).unique_ids;

        % ************************************************************************
        % Clean duplicates  II = cellfun(@isequal,TTa,TTb)
        % ************************************************************************
        index2NoDuplicates = CleanDuplicates(originalDataStruct(j).user_ids);  
        % *************************************************************************  

        combinedDataStruct(end).asins = originalDataStruct(j).asins;
        combinedDataStruct(end).parent_asins = originalDataStruct(j).parent_asins;
        combinedDataStruct(end).ratings = originalDataStruct(j).ratings(index2NoDuplicates);
        combinedDataStruct(end).user_ids = originalDataStruct(j).user_ids(index2NoDuplicates);
        combinedDataStruct(end).verified_purchases = originalDataStruct(j).verified_purchases(index2NoDuplicates);
        combinedDataStruct(end).helpful_votes = originalDataStruct(j).helpful_votes(index2NoDuplicates);
        num_reviews = length(originalDataStruct(j).ratings(index2NoDuplicates));

        % Convert timestamp from Unix to datetime
        combinedDataStruct(end).timestamps = datetime(originalDataStruct(j).timestamps(index2NoDuplicates), 'ConvertFrom', 'posixtime');
        
        % Convert images to binary
    if all(cellfun(@isempty, originalDataStruct(j).images(index2NoDuplicates)))
         combinedDataStruct(end).images = 0;  
    else
         combinedDataStruct(end).images = 1;  
    end
        
        % Calculate Time Since First Review in days
        first_review_time = combinedDataStruct(end).timestamps(1);
        days_since_first_review = days(combinedDataStruct(end).timestamps - first_review_time);
        combinedDataStruct(end).days_since_first_review = days_since_first_review;

        time_diff_matrix = (combinedDataStruct(end).days_since_first_review -  combinedDataStruct(end).days_since_first_review');
  
       % Create delta time between reviews
       combinedDataStruct(end).reviews_delta_time = [0,diff(combinedDataStruct(end).days_since_first_review)];
        
        % Calculate Total Number of Previous Reviews (with differnt time
        % acuuracy

        for days_window = 0:7
          % Calculate Number of Previous Reviews By rating for the current time window
            num_prev_stars_for_days = zeros(5,num_reviews);
            num_prev_stars_for_days(1,:) = sum((time_diff_matrix > days_window) & (combinedDataStruct(end).ratings' == 1));
            num_prev_stars_for_days(2,:) = sum((time_diff_matrix > days_window) & (combinedDataStruct(end).ratings' == 2));
            num_prev_stars_for_days(3,:) = sum((time_diff_matrix > days_window) & (combinedDataStruct(end).ratings' == 3));
            num_prev_stars_for_days(4,:) = sum((time_diff_matrix > days_window) & (combinedDataStruct(end).ratings' == 4));
            num_prev_stars_for_days(5,:) = sum((time_diff_matrix > days_window) & (combinedDataStruct(end).ratings' == 5));
            combinedDataStruct(end).(sprintf('num_prev_stars_for_%ddays', days_window)) = num_prev_stars_for_days;

         
        end


        combinedDataStruct(end).total_num_prev_reviews =sum(time_diff_matrix>0);

        combinedDataStruct(end).total_num_prev_reviews_for_days =[ sum(time_diff_matrix>1);
                                                                   sum(time_diff_matrix>2);
                                                                   sum(time_diff_matrix>3);
                                                                   sum(time_diff_matrix>4);
                                                                   sum(time_diff_matrix>5);
                                                                   sum(time_diff_matrix>6);
                                                                   sum(time_diff_matrix>7)
        ];         
        % 


 
        delta_time_OutOfWindow = zeros(7,num_reviews);
        for window =  1:7
            % Create a binary matrix representing if the time difference is at least the current window size
            out_of_window = time_diff_matrix >= window;

         % Find the indices where the time difference is greater than or equal to the current window size
          Index = sum(out_of_window);

          % Set the first values in Index to 1, which is the first valid review reference
           if all(Index == 0)
              Index = ones(size(Index));  % If no indices found, set all to 1
           else
              Index(1:find(Index > 0, 1) - 1) = 1;  % Set first values to 1
           end

            % Dynamically create the field name for delta_time based on the current window size
            delta_field = sprintf('delta_time_OutOfWindow_%d', window);

             % Calculate the time difference between the current review and the previous review that was at least 'window' days before
             delta_time_OutOfWindow(window,:) = (combinedDataStruct(end).days_since_first_review - combinedDataStruct(end).days_since_first_review(Index)).*(combinedDataStruct(end).days_since_first_review>window);
        end
        combinedDataStruct(end).delta_time_OutOfWindow = delta_time_OutOfWindow;




        % Calculate Text Size
        combinedDataStruct(end).text_size = cellfun(@length, originalDataStruct(j).texts(index2NoDuplicates));

        % Calculate IsLastReview (same days last review will also be
        % consider last review)
        combinedDataStruct(end).is_last_review = (round(days_since_first_review) == round(days_since_first_review(end)));

        % Calculate IsLastReview_week (same week last review will also be
        % consider last review)
        combinedDataStruct(end).is_last_review_week = (days_since_first_review >= (days_since_first_review(end) - 7));
        
        % Calculate Number of Simultaneous Reviews - ***SAME DAY***       
        [unique_dates, ~, idx] = unique(round(days_since_first_review));
        counts = accumarray(idx, 1);
        combinedDataStruct(end).num_simultaneous_reviews = counts(idx)';
        catch ME
              if contains(ME.message, 'exceeds maximum array size preference')
                    fprintf(2, 'Error: Time difference matrix is too large to compute for review with asin: %s\n', combinedDataStruct(end).asins);
                    reviewData  =originalDataStruct(j);
                     asin = originalDataStruct(j).asins;
                     largeReviewFile = fullfile(largeReviewDir, [asin, '.mat']);
                     save(largeReviewFile, 'reviewData', '-v7.3');
                     fprintf('Saved large review to %s', largeReviewFile);
              else
                     rethrow(ME);
              end
              combinedDataStruct = struct('unique_ids', {}, 'asins', {}, 'parent_asins', {}, 'ratings', {}, 'user_ids', {}, 'verified_purchases', {}, 'helpful_votes', {}, 'timestamps', {}, 'images', {}, ...
    'days_since_first_review', {},'reviews_delta_time', {}, 'total_num_prev_reviews', {},'total_num_prev_reviews_for_days', {}, ...
    'num_prev_stars_for_0days', {},'num_prev_stars_for_1days', {},'num_prev_stars_for_2days', {},'num_prev_stars_for_3days', {},'num_prev_stars_for_4days', {},'num_prev_stars_for_5days', {},'num_prev_stars_for_6days', {},'num_prev_stars_for_7days', {}, ...
    'delta_time_OutOfWindow', {}, 'text_size', {}, 'is_last_review', {}, 'is_last_review_week', {}, 'num_simultaneous_reviews', {});
        end
    end
    
    % Increment the processed files counter
    processedFilesCounter = processedFilesCounter + 1;
    
    % If 10 files have been processed, save the combinedDataStruct
    if mod(processedFilesCounter, 1) == 0
        startIdx = (i - 1) * chunkSize + 1;
        endIdx = i * chunkSize;
        outputFileName = fullfile(outputDir, sprintf('All_Beauty_processed%d_%d.mat', startIdx, endIdx));
        
        % Save the new dataStruct to a .mat file
        save(outputFileName, 'combinedDataStruct', '-v7.3');
        fprintf('Saved reduced form to %s\n', outputFileName);
        
        % Reset the combinedDataStruct for the next batch
        combinedDataStruct = struct('unique_ids', {}, 'asins', {}, 'parent_asins', {}, 'ratings', {}, 'user_ids', {}, 'verified_purchases', {}, 'helpful_votes', {}, 'timestamps', {}, 'images', {}, ...
          'days_since_first_review', {},'reviews_delta_time', {}, 'total_num_prev_reviews', {},'total_num_prev_reviews_for_days', {}, ...
          'num_prev_stars_for_0days', {},'num_prev_stars_for_1days', {},'num_prev_stars_for_2days', {},'num_prev_stars_for_3days', {},'num_prev_stars_for_4days', {},'num_prev_stars_for_5days', {},'num_prev_stars_for_6days', {},'num_prev_stars_for_7days', {}, ...
          'delta_time_OutOfWindow', {}, 'text_size', {}, 'is_last_review', {}, 'is_last_review_week', {}, 'num_simultaneous_reviews', {});
    end
end

% Save any remaining data after the last iteration
if ~isempty(combinedDataStruct)
    startIdx = (processedFilesCounter - mod(processedFilesCounter, 10)) * chunkSize + 1;
    endIdx = processedFilesCounter * chunkSize;
    outputFileName = fullfile(outputDir, sprintf(['All_Beauty' ...
        '.0' ...
        '0..0_%d_%d_processed.mat'], startIdx, endIdx));
    
    % Save the new dataStruct to a .mat file
    save(outputFileName, 'combinedDataStruct', '-v7.3');
    fprintf('Saved reduced form to %s\n', outputFileName);
end

fprintf('Processing complete.\n');

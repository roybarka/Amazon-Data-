% reduced_form2matrix_PseudoScript_20241228.m is a pseudocode that constructs a generalized review matrix, with each row representing an individual review.

clear all;


TIMEINF = datetime('01-OCT-2023');
% Define the directory containing the original .mat files
inputDir = 'F:\AmazonData_Part1Full\Categories\Automotive\Procceced_Mat_Files';
outputDir = 'F:\AmazonData_Part1Full\Categories\Automotive\Matrix';
lookupTabledir = 'F:\AmazonData_Part1Full\Categories\Automotive\MatrixLookUp';

% Get a list of all .mat files in the input directory
matFiles = dir(fullfile(inputDir, '*.mat'));

% Extract numerical parts from filenames and sort
fileNumbers = cellfun(@(x) sscanf(x, 'Automotive_processed_%d_%*d.mat'), {matFiles.name});
[~, sortIdx] = sort(fileNumbers);
matFiles = matFiles(sortIdx);

for i = 1:length(matFiles)
    inputFile = fullfile(inputDir, matFiles(i).name);
    fprintf('%s Processing %s\n', datetime("now"),inputFile);
     
    % Load the existing dataStruct from the .mat file
    loadedData = load(inputFile);
    fprintf('loaded %s at %s\n',inputFile, datetime("now"));
    combinedDataStruct = loadedData.combinedDataStruct;
    combinedDataMatrix = [];
    combinedlookupMatrix = [];


for j = 1:length(combinedDataStruct)

      CurrentStartRow =  size(combinedDataMatrix,1) + 1;

      TotalNumReviews = length(combinedDataStruct(j).total_num_prev_reviews);
      Unique_id = combinedDataStruct(j).unique_ids;

% ****************************************************************************
% Column 1: Header
% ****************************************************************************
      ProductMatrix = repmat(Unique_id,TotalNumReviews,1);

% ****************************************************************************
% The Independent Variables
% ****************************************************************************
% ****************************************************************************
% Columns 2-9: Total number of previous reviews within the time windows of 0 to 7 days.
% Data taken from:
% combinedDataStruct(j).total_num_prev_reviews and 
% combinedDataStruct(j).total_num_prev_reviews_for_days
% ****************************************************************************
      ProductMatrix = [ProductMatrix, combinedDataStruct(j).total_num_prev_reviews', combinedDataStruct(j).total_num_prev_reviews_for_days'];

% ****************************************************************************
% Columns 10-14: Total number of previous reviews for a given star rating within a 0-day window.
% Source: combinedDataStruct(j).num_prev_stars_for_0days
% Columns 15-18: Computed values for average rating, standard deviation, 
% percentage of attractors and detractors.
% ****************************************************************************
      num_prev_stars_for_0days = combinedDataStruct(j).num_prev_stars_for_0days';

% Calculate the average rating:
      maxNN1 = max(sum(num_prev_stars_for_0days, 2), 1);
       mean_prev_stars_for_0days = sum(num_prev_stars_for_0days .* [1:5], 2) ./ maxNN1;

% Calculate the rating's standard deviation:
      maxNN2 = max(sum(num_prev_stars_for_0days, 2), 2) - 1;
      std_prev_stars_for_0days = sqrt(sum(num_prev_stars_for_0days .* (([1:5] - mean_prev_stars_for_0days) .^ 2), 2) ./ maxNN2);

% Calculate the percentages of attractors and detractors:
      Attractors_for_0days = num_prev_stars_for_0days(:, 5) ./ maxNN1;
      Detractors_for_0days = sum(num_prev_stars_for_0days(:, 1:3), 2) ./ maxNN1;

% Add columns to ProductMatrix:
      ProductMatrix = [ProductMatrix, num_prev_stars_for_0days, mean_prev_stars_for_0days, std_prev_stars_for_0days, Attractors_for_0days, Detractors_for_0days];

% ****************************************************************************
% Columns 19-23: Total number of previous reviews for a given star rating within a 1-day window.
% Source: combinedDataStruct(j).num_prev_stars_for_1days
% Columns 24-27: Computed values for average rating, standard deviation, 
% percentage of attractors and detractors.
% ****************************************************************************
      num_prev_stars_for_1days = combinedDataStruct(j).num_prev_stars_for_1days';

% Calculate the average rating:
      maxNN1 = max(sum(num_prev_stars_for_1days, 2), 1);
mean_prev_stars_for_1days = sum(num_prev_stars_for_1days .* [1:5], 2) ./ maxNN1;

% Calculate the rating's standard deviation:
      maxNN2 = max(sum(num_prev_stars_for_1days, 2), 2) - 1;
      std_prev_stars_for_1days = sqrt(sum(num_prev_stars_for_1days .* (([1:5] - mean_prev_stars_for_1days) .^ 2), 2) ./ maxNN2);

% Calculate the percentages of attractors and detractors:
      Attractors_for_1days = num_prev_stars_for_1days(:, 5) ./ maxNN1;
      Detractors_for_1days = sum(num_prev_stars_for_1days(:, 1:3), 2) ./ maxNN1;

% Add columns to ProductMatrix:
      ProductMatrix = [ProductMatrix, num_prev_stars_for_1days, mean_prev_stars_for_1days, std_prev_stars_for_1days, Attractors_for_1days, Detractors_for_1days];

% ****************************************************************************
% Columns 28-32: Total number of previous reviews for a given star rating within a 2-days window.
% Source: combinedDataStruct(j).num_prev_stars_for_2days
% Columns 33-36: Computed values for average rating, standard deviation, 
% percentage of attractors and detractors.
% ****************************************************************************
      num_prev_stars_for_2days = combinedDataStruct(j).num_prev_stars_for_2days';

% Calculate the average rating:
      maxNN1 = max(sum(num_prev_stars_for_2days, 2), 1);
mean_prev_stars_for_2days = sum(num_prev_stars_for_2days .* [1:5], 2) ./ maxNN1;

% Calculate the rating's standard deviation:
      maxNN2 = max(sum(num_prev_stars_for_2days, 2), 2) - 1;
      std_prev_stars_for_2days = sqrt(sum(num_prev_stars_for_2days .* (([1:5] - mean_prev_stars_for_2days) .^ 2), 2) ./ maxNN2);

% Calculate the percentages of attractors and detractors:
      Attractors_for_2days = num_prev_stars_for_2days(:, 5) ./ maxNN1;
      Detractors_for_2days = sum(num_prev_stars_for_2days(:, 1:3), 2) ./ maxNN1;

% Add columns to ProductMatrix:
      ProductMatrix = [ProductMatrix, num_prev_stars_for_2days, mean_prev_stars_for_2days, std_prev_stars_for_2days, Attractors_for_2days, Detractors_for_2days];


% ****************************************************************************
% Columns 37-41: Total number of previous reviews for a given star rating within a 3-days window.
% Source: combinedDataStruct(j).num_prev_stars_for_3days
% Columns 42-45: Computed values for average rating, standard deviation, 
% percentage of attractors and detractors.
% ****************************************************************************
      num_prev_stars_for_3days = combinedDataStruct(j).num_prev_stars_for_3days';

% Calculate the average rating:
      maxNN1 = max(sum(num_prev_stars_for_3days, 2), 1);
mean_prev_stars_for_3days = sum(num_prev_stars_for_3days .* [1:5], 2) ./ maxNN1;

% Calculate the rating's standard deviation:
      maxNN2 = max(sum(num_prev_stars_for_3days, 2), 2) - 1;
      std_prev_stars_for_3days = sqrt(sum(num_prev_stars_for_3days .* (([1:5] - mean_prev_stars_for_3days) .^ 2), 2) ./ maxNN2);

% Calculate the percentages of attractors and detractors:
      Attractors_for_3days = num_prev_stars_for_3days(:, 5) ./ maxNN1;
      Detractors_for_3days = sum(num_prev_stars_for_3days(:, 1:3), 2) ./ maxNN1;

% Add columns to ProductMatrix:
      ProductMatrix = [ProductMatrix, num_prev_stars_for_3days, mean_prev_stars_for_3days, std_prev_stars_for_3days, Attractors_for_3days, Detractors_for_3days];


% ****************************************************************************
% Columns 46-50: Total number of previous reviews for a given star rating within a 4-days window.
% Source: combinedDataStruct(j).num_prev_stars_for_4days
% Columns 51-54: Computed values for average rating, standard deviation, 
% percentage of attractors and detractors.
% ****************************************************************************
      num_prev_stars_for_4days = combinedDataStruct(j).num_prev_stars_for_4days';

% Calculate the average rating:
      maxNN1 = max(sum(num_prev_stars_for_4days, 2), 1);
mean_prev_stars_for_4days = sum(num_prev_stars_for_4days .* [1:5], 2) ./ maxNN1;

% Calculate the rating's standard deviation:
      maxNN2 = max(sum(num_prev_stars_for_4days, 2), 2) - 1;
      std_prev_stars_for_4days = sqrt(sum(num_prev_stars_for_4days .* (([1:5] - mean_prev_stars_for_4days) .^ 2), 2) ./ maxNN2);

% Calculate the percentages of attractors and detractors:
      Attractors_for_4days = num_prev_stars_for_4days(:, 5) ./ maxNN1;
      Detractors_for_4days = sum(num_prev_stars_for_4days(:, 1:3), 2) ./ maxNN1;

% Add columns to ProductMatrix:
      ProductMatrix = [ProductMatrix, num_prev_stars_for_4days, mean_prev_stars_for_4days, std_prev_stars_for_4days, Attractors_for_4days, Detractors_for_4days];


% ****************************************************************************
% Columns 55-59: Total number of previous reviews for a given star rating within a 5-days window.
% Source: combinedDataStruct(j).num_prev_stars_for_5days
% Columns 60-63: Computed values for average rating, standard deviation, 
% percentage of attractors and detractors.
% ****************************************************************************
      num_prev_stars_for_5days = combinedDataStruct(j).num_prev_stars_for_5days';

% Calculate the average rating:
      maxNN1 = max(sum(num_prev_stars_for_5days, 2), 1);
mean_prev_stars_for_5days = sum(num_prev_stars_for_5days .* [1:5], 2) ./ maxNN1;

% Calculate the rating's standard deviation:
      maxNN2 = max(sum(num_prev_stars_for_5days, 2), 2) - 1;
      std_prev_stars_for_5days = sqrt(sum(num_prev_stars_for_5days .* (([1:5] - mean_prev_stars_for_5days) .^ 2), 2) ./ maxNN2);

% Calculate the percentages of attractors and detractors:
      Attractors_for_5days = num_prev_stars_for_5days(:, 5) ./ maxNN1;
      Detractors_for_5days = sum(num_prev_stars_for_5days(:, 1:3), 2) ./ maxNN1;

% Add columns to ProductMatrix:
      ProductMatrix = [ProductMatrix, num_prev_stars_for_5days, mean_prev_stars_for_5days, std_prev_stars_for_5days, Attractors_for_5days, Detractors_for_5days];


% ****************************************************************************
% Columns 64-68: Total number of previous reviews for a given star rating within a 6-days window.
% Source: combinedDataStruct(j).num_prev_stars_for_6days
% Columns 69-72: Computed values for average rating, standard deviation, 
% percentage of attractors and detractors.
% ****************************************************************************
      num_prev_stars_for_6days = combinedDataStruct(j).num_prev_stars_for_6days';

% Calculate the average rating:
      maxNN1 = max(sum(num_prev_stars_for_6days, 2), 1);
mean_prev_stars_for_6days = sum(num_prev_stars_for_6days .* [1:5], 2) ./ maxNN1;

% Calculate the rating's standard deviation:
      maxNN2 = max(sum(num_prev_stars_for_6days, 2), 2) - 1;
      std_prev_stars_for_6days = sqrt(sum(num_prev_stars_for_6days .* (([1:5] - mean_prev_stars_for_6days) .^ 2), 2) ./ maxNN2);

% Calculate the percentages of attractors and detractors:
      Attractors_for_6days = num_prev_stars_for_6days(:, 5) ./ maxNN1;
      Detractors_for_6days = sum(num_prev_stars_for_6days(:, 1:3), 2) ./ maxNN1;

% Add columns to ProductMatrix:
      ProductMatrix = [ProductMatrix, num_prev_stars_for_6days, mean_prev_stars_for_6days, std_prev_stars_for_6days, Attractors_for_6days, Detractors_for_6days];


% ****************************************************************************
% Columns 73-77: Total number of previous reviews for a given star rating within a 7-days window.
% Source: combinedDataStruct(j).num_prev_stars_for_7days
% Columns 78-81: Computed values for average rating, standard deviation, 
% percentage of attractors and detractors.
% ****************************************************************************
      num_prev_stars_for_7days = combinedDataStruct(j).num_prev_stars_for_7days';

% Calculate the average rating:
      maxNN1 = max(sum(num_prev_stars_for_7days, 2), 1);
mean_prev_stars_for_7days = sum(num_prev_stars_for_7days .* [1:5], 2) ./ maxNN1;

% Calculate the rating's standard deviation:
      maxNN2 = max(sum(num_prev_stars_for_7days, 2), 2) - 1;
      std_prev_stars_for_7days = sqrt(sum(num_prev_stars_for_7days .* (([1:5] - mean_prev_stars_for_7days) .^ 2), 2) ./ maxNN2);

% Calculate the percentages of attractors and detractors:
      Attractors_for_7days = num_prev_stars_for_7days(:, 5) ./ maxNN1;
      Detractors_for_7days = sum(num_prev_stars_for_7days(:, 1:3), 2) ./ maxNN1;

% Add columns to ProductMatrix:
      ProductMatrix = [ProductMatrix, num_prev_stars_for_7days, mean_prev_stars_for_7days, std_prev_stars_for_7days, Attractors_for_7days, Detractors_for_7days];


% ****************************************************************************
% The Deependent Variables
% ****************************************************************************
% ****************************************************************************
% Source:  combinedDataStruct:
% Column 82: combinedDataStruct(j).days_since_first_review'
% Column 83: combinedDataStruct(j).reviews_delta_time'
% Colimns 84-90: combinedDataStruct(j).delta_time_OutOfWindow' (for 1-day up to 7-days window)
% Column 91: combinedDataStruct(j).ratings'
% Column 92: combinedDataStruct(j).verified_purchases'
% Column 93: combinedDataStruct(j).helpful_votes'
% Column 94: combinedDataStruct(j).text_size'
% Column 95: year(combinedDataStruct(j).timestamps')
% Column 96: month(combinedDataStruct(j).timestamps')
% Column 97: day(combinedDataStruct(j).timestamps')
% Column 98: weekday(combinedDataStruct(j).timestamps') 
% ****************************************************************************       
      timestamps = combinedDataStruct(j).timestamps';  
      ratings = combinedDataStruct(j).ratings';
      ProductMatrix = [ProductMatrix, combinedDataStruct(j).days_since_first_review', combinedDataStruct(j).reviews_delta_time',combinedDataStruct(j).delta_time_OutOfWindow', ratings, combinedDataStruct(j).verified_purchases',  combinedDataStruct(j).helpful_votes',  combinedDataStruct(j).text_size', year(timestamps), month(timestamps), day(timestamps), weekday(timestamps)];   

% ****************************************************************************
% Column 99: The 'is censored?' indicators all have the value of false.
% ****************************************************************************
      ProductMatrix = [ProductMatrix, false(TotalNumReviews,1)];
% ****************************************************************************
% Add the "consored row":
% ****************************************************************************
      censored_day_since_last_review = days(TIMEINF - timestamps(end)); 
      if censored_day_since_last_review>7

           % Prepare the values for columns 2-9: Columns 2 to 9 in the censored row should contain the total number of reviews.
           % Note that the time of censoring falls outside of all specified timeframes (0 to 7 days) relative to the last review. Hence the conatin the SAME value.
           total_num_reviews = repmat(TotalNumReviews,1,8);

           % Prepare the values for columns 10-18, which contain the review data for different star ratings. 
           num_reviews_1stars = sum(ratings==1);
           num_reviews_2stars = sum(ratings==2);
           num_reviews_3stars = sum(ratings==3);
           num_reviews_4stars = sum(ratings==4);
           num_reviews_5stars = sum(ratings==5);
           mean_review_ratings = mean(ratings);
           std_review_ratings = std(ratings);
           attractors = sum(ratings==5)/TotalNumReviews;
           dettractors = sum(ratings<=3)/TotalNumReviews;
           reviews_stars_data = [num_reviews_1stars, num_reviews_2stars, num_reviews_3stars, num_reviews_4stars, num_reviews_5stars,  mean_review_ratings, std_review_ratings,  attractors, dettractors];

           % Columns 19-27, 28-36, 37-45, 46-54, 55-63, 64-72, 73-81 are all replications of columns 10-18 as the time of censoring falls outside of all specified timeframes (0 to 7 days) relative to the last review.
           reviews_stars_data = repmat(reviews_stars_data,1,8);           

           % Column 82: The variable 'days since first review' is recorded as the  days passed between the day of censoring and the day of the first review.
           days_since_first_review = days(TIMEINF - timestamps(1));

           % Columns 83-90 are ALL  contain the SAME value, the value of censored_day_since_last_review that is the days passed between the day of censoring and the day of the last review.
           days_since_last_review = repmat(censored_day_since_last_review,1,8);

           % Clumnns 91-98 are ALL contain the value of 0  because there is NO current data in the censored row.
           % Colum 99 is the censoring indicator and takes the value of TRUE. 
           no_current_data_in_censored_row = [repmat(0,1,8), true]; 

           % Construct the "censored rwo":
           CensoredRow = [Unique_id, total_num_reviews, reviews_stars_data, days_since_first_review, days_since_last_review, no_current_data_in_censored_row];
           %   Add CensoredRow to ProductMatrix:
           ProductMatrix = [ProductMatrix ; CensoredRow];

      end % Add censored row    
      
% ***************************************************************************
% Column 100: An additional column: The row in the combineDataMatrix where the jth product  first review is provided.
% ************************************************************************** 

      NumOfProductMatrixRows = size(ProductMatrix,1);
      ProductMatrix = [ProductMatrix, repmat(CurrentStartRow, NumOfProductMatrixRows,1)];

 % ***************************************************************************
% Column 101: An additional column: The total number of reviews of the jth product.
% ************************************************************************** 
      ProductMatrix = [ProductMatrix, repmat(TotalNumReviews, NumOfProductMatrixRows,1)];    

% ***************************************************************************
% Column 102: An additional column: The jth product launch year:
% ************************************************************************** 
      LaunchYear = year(combinedDataStruct(j).timestamps(1));
      ProductMatrix = [ProductMatrix, repmat(LaunchYear, NumOfProductMatrixRows,1)];         

% ****************************************************************************
% Add the ProductMatrix (of the product j) to the combinedDataMatrix:
% ****************************************************************************
      combinedDataMatrix = [combinedDataMatrix ;  ProductMatrix];

% ****************************************************************************
% Update lookup matrix:
% ****************************************************************************
       CurrentEndRow = CurrentStartRow +  NumOfProductMatrixRows -1;
       combinedlookupMatrix = [combinedlookupMatrix ; [Unique_id CurrentStartRow CurrentEndRow]];
end
        outputlookupMatrixName = matFiles(i).name;
        outputlookupMatrixName = strrep(outputlookupMatrixName,'processed','lookupMatrix');     
        outputMatrixName = matFiles(i).name;
        outputMatrixName = strrep(outputMatrixName,'processed','Matrix'); 

        outputlookupFileName = fullfile(lookupTabledir, outputlookupMatrixName);
        outputFileName = fullfile(outputDir, outputMatrixName);

        % Save LookupTable
        %fprintf('%s starting to save %s\n', datetime("now"), outputlookupMatrixName);
        save(outputlookupFileName, 'combinedlookupMatrix', '-v7.3');
        %fprintf('%s Saved LookupTable to %s\n', datetime("now"), outputlookupFileName);
        
       % Save Matrix
        %fprintf('%s starting to save %s\n', datetime("now"), outputMatrixName);
        save(outputFileName, 'combinedDataMatrix', '-v7.3');
        fprintf('%s Saved Matrix to %s\n',datetime("now"), outputFileName);
end
% load('G:\AmazonData\LookUp Table\ElectronicsLookUpTable_Enriched_SparseMatrix.mat')
% load('G:\AmazonData\LookUp Table\ElectronicsLookUpTable_Enriched.mat')
% load('G:\AmazonData\Meta_Data LookUp Table\meta_Electronics_Category_Lookup.mat')
function ViewProductDetails(unique_id, lookupTableStruct, categoryLookupTable);
    str = sprintf('unique_id = %d\nasin = %s\nparent_asin= %s\n',...
        unique_id,...
        lookupTableStruct(unique_id).asin,...
        lookupTableStruct(unique_id).parent_asin);
    str = [str, sprintf('category_ids:\n')];
    for i = 1:length(lookupTableStruct(unique_id).category_ids)
        category_idx = find(table2array(categoryLookupTable(:,1)) == lookupTableStruct(unique_id).category_ids(i));
        catName = categoryLookupTable{category_idx, 2};
        catName = catName{1};
        str = [str, sprintf(' - %s\n', catName)];
    end
    str
end




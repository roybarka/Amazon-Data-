function index2NoDuplicates = CleanDuplicates(user_ids)
      index2NoDuplicates = 1;
      if length(user_ids)>1
            user_ids_left = user_ids(1:end-1); 
            user_ids_right = user_ids(2:end); 
            DuplicateFlags = cellfun(@isequal,user_ids_left,user_ids_right);
            DuplicateFlags = [false,  DuplicateFlags];
            index2NoDuplicates = find(~DuplicateFlags);
      end
end

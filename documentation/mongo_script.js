// results indicator row
print("my results start from here");

// header row
print("event,row_count");

db.mixpanel_stage.aggregate(
   [
      {
        $group : {
           _id : { event: "$event"},
           count: { $sum: 1 }
        }
      }
   ]
).forEach(function(row) {
    print(row._id.event+","+row.count);
    });
// find documents with duplicat names, and output their visible and name fields

db.dataproducts.aggregate([
  {
    $group: {
      _id: "$name",
      count: {$sum: 1},
      documents: {$push: {name: "$name", visible: "$visible"}}
    }
  },
  {
    $match: {
      count: {$gt: 1}
    }
  },
  {
    $project: {
      _id: 0,
      duplicates: "$documents"
    }
  }
])

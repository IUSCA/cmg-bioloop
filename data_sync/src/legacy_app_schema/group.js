const mongoose = require('mongoose');
var Schema = mongoose.Schema;
const winston = require('winston');
const config = require('../config');
const logger = new winston.createLogger(config.logger.winston);


var groupSchema = mongoose.Schema({
  ///////////////////////////////////////////////////////////////////////////
  ///////////////////////////////////////////////////////////////////////////
  name: { type: String, index: {unique: true}},
  pi: {type: mongoose.Schema.Types.ObjectId, ref: 'User'},
  desc: String,
  members: [{type: mongoose.Schema.Types.ObjectId, ref: 'User'}],
  active: {type: Boolean, default: true},

}, {timestamps: {createdAt: 'createdAt', updatedAt: 'updatedAt'}, strict: false});


groupSchema.statics.getUserGroups = function(user, cb) {
  this.find({members: user.id}, function(err, groups) {
    if(err) return cb(err, null);
    var gids = [];
    groups.forEach(function(group) {
      gids.push(group.id);
    });
    logger.error(`User ${user.username} has group memberships in ${gids}`);
    cb(null, gids);
  })
};

exports.Group  = mongoose.model('Group', groupSchema);


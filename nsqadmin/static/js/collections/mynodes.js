var Backbone = require('backbone');

var AppState = require('../app_state');

var MyNodeModel = require('../models/mynode');

var MyNodes = Backbone.Collection.extend({
    model: MyNodeModel,

    comparator: 'id',

    constructor: function MyNodes() {
        Backbone.Collection.prototype.constructor.apply(this, arguments);
    },

    url: function() {
        return AppState.apiPath('/analyze');
    },

});

module.exports = MyNodes;

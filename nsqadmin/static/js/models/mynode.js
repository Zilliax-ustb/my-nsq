var AppState = require('../app_state');
var Backbone = require('backbone');

var MyNodeModel = Backbone.Model.extend({
    idAttribute: 'ip_address',

    constructor: function MyNode() {
        Backbone.Model.prototype.constructor.apply(this, arguments);
    },

    urlRoot: function() {
        return AppState.apiPath('/analyze');
    },

});

module.exports = MyNodeModel;

var $ = require('jquery');

var Pubsub = require('../lib/pubsub');
var AppState = require('../app_state');

var BaseView = require('./base');

var MyNodes = require('../collections/mynodes');

var AnalyzeView = BaseView.extend({
    className: 'mynodes container-fluid',

    template: require('./spinner.hbs'),

    events: {
        'click .conn-count': 'onClickConnCount'
    },

    initialize: function() {
        BaseView.prototype.initialize.apply(this, arguments);
        this.listenTo(AppState, 'change:graph_interval', this.render);
        this.collection = new MyNodes();
        this.collection.fetch()
            .done(function(data) {
                this.template = require('./analyze.hbs');
                this.render(data);
            }.bind(this))
            .fail(this.handleViewError.bind(this))
            .always(Pubsub.trigger.bind(Pubsub, 'view:ready'));
    },

    onClickConnCount: function(e) {
        e.preventDefault();
        $(e.target).next().toggle();
    }
});

module.exports = AnalyzeView;

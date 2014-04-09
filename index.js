var utile = require('utile')
	, url = require('url')
	, Q = require('q')
	, jsonld = require('jsonld').promises()
	, rest = require('rest')
	, Resolver = require('./Resolver')
;

var jsonConverter = require("rest/mime/type/application/json");
var textConverter = require("rest/mime/type/text/plain")

var sparqlJsonConverter = {
	read: function (str, opts) {
	    var obj = JSON.parse(str);
	    return obj.results.bindings
	},
	write: function (obj, opts) {
	    return JSON.stringify(str);
	}
};

var sparqlXmlConverter = {
	read: function (str, opts) {
	    return /(>true<\/)/.test(str);
	},
	write: function (obj, opts) {
	    return obj;
	}
};

var registry = require("rest/mime/registry");

registry.register("application/ld+json", jsonConverter)
registry.register("application/sparql-results+json", sparqlJsonConverter)
registry.register("application/sparql-results+xml", sparqlXmlConverter)

registry.register("application/nquads", textConverter)
registry.register("text/x-nquads", textConverter)
registry.register("text/turtle", textConverter)

var GraphStoreClient = module.exports = function(endpoint, graphStoreEndpoint){

	this.endpoint = endpoint;
	this.graphStoreEndpoint = graphStoreEndpoint;
	this.ns = new Resolver;
	this._request = rest
		.chain(require("rest/interceptor/mime"), {accept: "application/ld+json,application/sparql-results+json,application/json,*/*", mime:"text/x-nquads"})
		.chain(sparqlInterceptor())
	this._del_request = rest
		.chain(sparqlInterceptor())
}

GraphStoreClient.prototype = {
	query: function(sparql, bindings){
		if(bindings && bindings instanceof Object){
			for(var i in bindings){
				sparql = sparql.replace(new RegExp('(\\?|\\$)('+i+')', 'g'), bindings[i]);
			}
		}
		var prefixes = this.ns.base ? utile.format("BASE <%s>\n", this.ns.base) : "";
		for(var i in this.ns.prefixes){
			prefixes += utile.format("PREFIX %s <%s>\n", i, this.ns.prefixes[i]);
		}
		sparql = prefixes + sparql;
		console.log(sparql);
		return this._request({
			path: this.endpoint,
			params: {query: sparql},
			headers: {"SD-Connection-String": "reasoning=SL"}
		});
	},
	put: function(iri, graph, type){
		var type = type || "text/turtle", self = this;
		if(typeof graph == "object"){
			var type = "text/x-nquads";
			var graph = jsonld.toRDF(graph, {format: 'application/nquads'});
		}
		return Q.when(graph)
		.then(function(graph){
			return self._request({
				method: "PUT",
				path: self.graphStoreEndpoint,
				headers: {"Content-Type": type},
				params: {graph: url.resolve(self.ns.base, iri)},
				entity: graph
			});
		});
	},
	post: function(iri, graph, type){
		var type = type || "text/turtle", self = this;
		if(typeof graph == "object"){
			var type = "text/x-nquads";
			var graph = jsonld.toRDF(graph, {format: 'application/nquads'});
		}
		return Q.when(graph)
		.then(function(graph){
			return self._request({
				method: "POST",
				path: self.graphStoreEndpoint,
				headers: {"Content-Type": type},
				params: {graph: url.resolve(self.ns.base, iri)},
				entity: graph
			});
		});
	},

	delete: function(iri){
		return this._del_request({
			method: "DELETE",
			path: this.graphStoreEndpoint,
			params: {graph: url.resolve(this.ns.base, iri)},
		});
	},
	get: function(iri){
		return this._request
		({
			method: "GET",
			path: this.graphStoreEndpoint,
			params: {graph: url.resolve(this.ns.base, iri)},
		});
	}
}

function sparqlInterceptor(){
    return require('rest/interceptor')({
            response: function (response) {
                if (response.status && response.status.code >= 400) {
            		var e = {
            			message: "SPARQL Endpoint Error:" + response.status.code + " " + response.entity,
            			stack: "Request:\n" + 
            				JSON.stringify(response.request, null, " ") + "---------\nResponse:" + 
            				response.status.code +"\n" +JSON.stringify(response.headers, null, " ") +
            				response.entity,
            			status: response.status.code,
            			headers: response.headers,
            		}
                    return Q.reject(e);
                }
                return response.entity;
            }
    });

}

String.prototype.iri = function(base, bare){
	var v = base ? url.resolve(base, this + "") : this +"";
	return bare ? v : "<" + v + ">";
}
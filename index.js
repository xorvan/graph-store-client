var utile = require('utile')
	, url = require('url')
	, debug = require("debug")("graph-store-client")
	, Q = require('q')
	, jsonld = require('jsonld').promises()
	, rest = require('rest')
	, Resolver = require('./Resolver')
	, StringDecoder = require('string_decoder').StringDecoder
;

var jsonConverter = require("rest/mime/type/application/json");
var textConverter = require("rest/mime/type/text/plain")

var sparqlJsonConverter = {
	read: function (str, opts) {
	    var obj = JSON.parse(str);
	    return obj.boolean === undefined ? obj.results.bindings : obj.boolean;
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
registry.register("application/sparql-query", textConverter)
registry.register("application/sparql-update", textConverter)

var GraphStoreClient = module.exports = function(endpoint, graphStoreEndpoint){

	this.endpoint = endpoint;
	this.graphStoreEndpoint = graphStoreEndpoint;
	this.ns = new Resolver;
	this._request = rest
		.chain(require("rest/interceptor/mime"), {accept: "application/ld+json,application/x-trig,application/sparql-results+json,application/json,*/*", mime: "application/sparql-query"})
		.chain(sparqlInterceptor())
	this._del_request = rest
		.chain(sparqlInterceptor())
}

GraphStoreClient.prototype = {
	query: function(sparql, bindings){
		return this._sparql("application/sparql-query", sparql, bindings)
	},
	update: function(sparql, bindings){
		return this._sparql("application/sparql-update", sparql, bindings)
	},
	_sparql: function(type, sparql, bindings){
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

		debug("Running SPARQL: %s", sparql);
		return rest
		.chain(require("rest/interceptor/mime"), {accept: "application/ld+json,text/plain,application/sparql-results+json,application/json,*/*", mime: type+";charset=UTF-8"})
		.chain(sparqlInterceptor())
		({
			path: this.endpoint,
			mime: type,
			entity: sparql,
			headers: {
				"Accept-Charset": "utf-8"
			}
		})
		.catch(function(e){
			throw new Error("SPARQL Endpoint Error: (" + e.status + ") "+ e.message);
		})
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

function debugInterceptor(){
	return require('rest/interceptor')({
		response: function (response) {
				debug("SPARQL Result Response:", response.entity);
				return response;
		}
	});
}

function sparqlInterceptor(){
    return require('rest/interceptor')({
            response: function (response) {
								if(response.error){
									return Q.reject(response.error);
								}
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
								var decoder = new StringDecoder('utf8');
								var entity = response.entity + ""
								entity = unescape(entity.replace(/\\u/g, '%u') );

								debug("SPARQL Result Entity:", entity)
                return response.headers['Content-Type'].indexOf('text/plain') == 0 ? jsonld.fromRDF(entity, {format: 'application/nquads'}) : response.entity;
            }
    });

}

String.prototype.iri = function(base, bare){
	var v = base ? url.resolve(base, this + "") : this +"";
	return bare ? v : "<" + decodeURI(v) + ">";
}

String.prototype.lit = function(){
	return '"' + this + '"';
}

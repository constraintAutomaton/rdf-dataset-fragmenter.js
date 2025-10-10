import { DataFactory } from 'rdf-data-factory';

const DF = new DataFactory();

const a = DF.fromTerm({ value: 'aaa', termType: 'NamedNode' });
console.log(a);
console.log(a.equals(a));


"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.QuadTransformMultipleVocabulary = void 0;
const rdf_data_factory_1 = require("rdf-data-factory");
const fs = require("fs");
const DF = new rdf_data_factory_1.DataFactory();
/**
 * A quad transformer that generate quads into another vocabulary by data sources
 */
class QuadTransformMultipleVocabulary {
    constructor(args) {
        this.dataSetRuleAssoc = new Map();
        this.datasetPatterns = new RegExp(args.datasetPatterns, 'u');
        let rules = [];
        if (typeof args.rules === "string") {
            const data = fs.readFileSync(args.rules, 'utf-8');
            rules = JSON.parse(data)["rules"];
                        

        }
        else {
            rules = args.rules;
        }
        for (const ruleSet of rules) {
            for (const rule of ruleSet) {
                if (QuadTransformMultipleVocabulary.SAME_AS.value !== rule.inference.value) {
                    throw new Error(`${rule.inference.value} is not a suported inference`);
                }
                console.log(rule);
                if(rule.premise.termType === "Literal"){
                    rule.premise = DF.literal(rule.premise.value);
                }
                if(rule.inference.termType === "Literal"){
                    rule.inference = DF.literal(rule.inference.value);
                }
                if(rule.conclusion.termType === "Literal"){
                    rule.conclusion = DF.literal(rule.conclusion.value);
                }
                rule.premise = DF.fromTerm({...rule.premise, equals:function(el){return el.value === this.value}});
                rule.inference = DF.fromTerm({...rule.inference, equals:function(el){return el.value === this.value}});
                rule.conclusion = DF.fromTerm({...rule.conclusion, equals:function(el){return el.value === this.value}});
                console.log(rule.conclusion);
            }
        }
        this.rules = rules;
    }
    transform(quad) {
        const ruleSet = this.getRuleSet(quad);
        if (ruleSet === undefined) {
            return [quad];
        }
        // We cast because nothing stop a user to produce base quad instead of quads
        console.log("YES!")
        console.log(QuadTransformMultipleVocabulary.transfromQuadFromRuleSet(quad, ruleSet))
        return [
            QuadTransformMultipleVocabulary.transfromQuadFromRuleSet(quad, ruleSet),
        ];
    }
    /**
     * From a quad get the matching dataset
     * @param {RDF.Quad} quad
     * @returns {string | undefined} the matching dataset
     */
    getMatchingDataset(quad) {
        const subjectMatches = this.datasetPatterns.exec(quad.subject.value);
        if (subjectMatches !== null) {
            return subjectMatches[0];
        }
        const objectMatches = this.datasetPatterns.exec(quad.object.value);
        if (objectMatches !== null) {
            return objectMatches[0];
        }
        return undefined;
    }
    /**
     * Get a rule set that is relevant to a quad.
     * It first look if the subject of the quad is part of a dataset then
     * it look at the object.
     * If the quad is not part of a dataset then undefined is returned.
     * @param {RDF.Quad} quad
     * @returns {RuleSet | undefined} the relevant rule set to the quad.
     */
    getRuleSet(quad) {
        const dataset = this.getMatchingDataset(quad);
        if (dataset === undefined) {
            return undefined;
        }
        const ruleSet = this.dataSetRuleAssoc.get(dataset);
        if (ruleSet !== undefined) {
            return ruleSet;
        }
        const datasetToNumber = QuadTransformMultipleVocabulary.stringToNumberHash(dataset);
        const index = datasetToNumber % this.rules.length;
        const currentRuleSet = this.rules[index];
        this.dataSetRuleAssoc.set(dataset, currentRuleSet);
        return currentRuleSet;
    }
    static stringToNumberHash(str) {
        let hash = 0;
        for (let i = 0; i < str.length; i++) {
            hash = (hash << 5) - hash + str.charCodeAt(i);
            hash = Math.trunc(hash);
        }
        return Math.abs(hash);
    }
    /**
     * Transform a quad from a rule set.
     * @param {RDF.BaseQuad} quad
     * @param {RuleSet} ruleSet
     * @returns {RDF.BaseQuad}
     */
    static transfromQuadFromRuleSet(quad, ruleSet) {
        const resp = [];
        for (const rule of ruleSet) {
            const newQuad = this.transformQuad(quad, rule);
            resp.push(newQuad);
        }
        return QuadTransformMultipleVocabulary.mergeQuad(resp, quad);
    }
    /**
     * Merge the quads generated from the rules to produce the final quad.
     * It will take the final version of the quad for each terms.
     * This does not perform transitive rules.
     * It is not important because this module only defined rules into another vocabulary,
     * so there is no reason for us to not be direct.
     * @param {RDF.BaseQuad[]} quads
     * @param {RDF.BaseQuad} originalQuad
     * @returns {RDF.BaseQuad}
     */
    static mergeQuad(quads, originalQuad) {
        let subject = originalQuad.subject;
        let predicate = originalQuad.predicate;
        let object = originalQuad.object;
        for (const quad of quads) {
            if (!quad.subject.equals(originalQuad.subject)) {
                subject = quad.subject;
            }
            if (!quad.predicate.equals(originalQuad.predicate)) {
                predicate = quad.predicate;
            }
            if (!quad.object.equals(originalQuad.object)) {
                object = quad.object;
            }
        }
        return DF.quad(subject, predicate, object);
    }
    /**
     * Transform a quad given a rule.
     * @param {RDF.BaseQuad} quad
     * @param {IRule} rule
     * @returns {RDF.BaseQuad}
     */
    static transformQuad(quad, rule) {
        const subject = this.transformTerm(quad.subject, rule);
        const predicate = this.transformTerm(quad.predicate, rule);
        const object = this.transformTerm(quad.object, rule);
        return DF.quad(subject, predicate, object);
    }
    /**
     * Transform a RDF term based on the reverse of a rule.
     * Cannot work if the rule cannot be reverted.
     * @param {RDF.Term} term an RDF term
     * @param {IRule} rule A rule
     * @returns {RDF.Term} The premise of the rule
     */
    static transformTerm(term, rule) {
        if (rule.conclusion.equals(term) &&
            rule.inference.equals(QuadTransformMultipleVocabulary.SAME_AS)) {
            return rule.premise;
        }
        return term;
    }
}
exports.QuadTransformMultipleVocabulary = QuadTransformMultipleVocabulary;
QuadTransformMultipleVocabulary.SAME_AS = DF.namedNode('http://www.w3.org/2002/07/owl#sameAs');
//# sourceMappingURL=QuadTransformMultipleVocabulary.js.map
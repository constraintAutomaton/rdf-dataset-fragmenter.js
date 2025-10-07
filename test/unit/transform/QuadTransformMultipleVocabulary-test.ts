import {
  IRule,
  QuadTransformMultipleVocabulary,
  RuleSet,
} from "../../../lib/transform/QuadTransformMultipleVocabulary";
import * as RDF from "@rdfjs/types";
import { DataFactory } from "rdf-data-factory";

const DF = new DataFactory<RDF.BaseQuad>();

describe(QuadTransformMultipleVocabulary.name, () => {
  describe(QuadTransformMultipleVocabulary.transformTerm.name, () => {
    it("should return the premise of a rule given the term targeted is the conclusion", () => {
      const term = DF.namedNode("c");
      const rule: IRule = {
        premise: DF.namedNode("p"),
        inference: QuadTransformMultipleVocabulary.SAME_AS,
        conclusion: DF.namedNode("c"),
      };
      const resp = QuadTransformMultipleVocabulary.transformTerm(term, rule);
      expect(resp).toEqual(DF.namedNode("p"));
    });

    it("should return the term given a rule not supported", () => {
      const term = DF.namedNode("c");
      const rule: IRule = {
        premise: DF.namedNode("p"),
        inference: DF.namedNode("i"),
        conclusion: DF.namedNode("c"),
      };
      const resp = QuadTransformMultipleVocabulary.transformTerm(term, rule);
      expect(resp).toEqual(term);
    });

    it("should return the term given an unrelated term", () => {
      const term = DF.namedNode("z");
      const rule: IRule = {
        premise: DF.namedNode("p"),
        inference: QuadTransformMultipleVocabulary.SAME_AS,
        conclusion: DF.namedNode("c"),
      };
      const resp = QuadTransformMultipleVocabulary.transformTerm(term, rule);
      expect(resp).toEqual(term);
    });
  });

  describe(QuadTransformMultipleVocabulary.transformQuad.name, () => {
    it("should transform the quad given a fully matching rule", () => {
      const quad: RDF.BaseQuad = DF.quad(
        DF.namedNode("c"),
        DF.namedNode("c"),
        DF.namedNode("c")
      );
      const expectedQuad = DF.quad(
        DF.namedNode("p"),
        DF.namedNode("p"),
        DF.namedNode("p")
      );
      const rule: IRule = {
        premise: DF.namedNode("p"),
        inference: QuadTransformMultipleVocabulary.SAME_AS,
        conclusion: DF.namedNode("c"),
      };

      const resp = QuadTransformMultipleVocabulary.transformQuad(quad, rule);

      expect(resp).toEqual(expectedQuad);
    });

    it("should transform the quad given a partially matching rule", () => {
      const quad: RDF.BaseQuad = DF.quad(
        DF.namedNode("z"),
        DF.namedNode("p"),
        DF.namedNode("c")
      );
      const expectedQuad = DF.quad(
        DF.namedNode("z"),
        DF.namedNode("p"),
        DF.namedNode("p")
      );
      const rule: IRule = {
        premise: DF.namedNode("p"),
        inference: QuadTransformMultipleVocabulary.SAME_AS,
        conclusion: DF.namedNode("c"),
      };

      const resp = QuadTransformMultipleVocabulary.transformQuad(quad, rule);

      expect(resp).toEqual(expectedQuad);
    });

    it("should transform the quad given a non-matching rule", () => {
      const quad: RDF.BaseQuad = DF.quad(
        DF.namedNode("z"),
        DF.namedNode("p"),
        DF.namedNode("q")
      );
      const rule: IRule = {
        premise: DF.namedNode("p"),
        inference: QuadTransformMultipleVocabulary.SAME_AS,
        conclusion: DF.namedNode("c"),
      };

      const resp = QuadTransformMultipleVocabulary.transformQuad(quad, rule);

      expect(resp).toEqual(quad);
    });
  });

  describe(QuadTransformMultipleVocabulary.transfromQuadFromRuleSet.name, () => {
    it("should produce a quads given multiple matching rules", () => {
      const quad: RDF.BaseQuad = DF.quad(
        DF.namedNode("c1"),
        DF.namedNode("c2"),
        DF.namedNode("c3")
      );
      const expectedQuad = DF.quad(DF.namedNode("p1"), DF.namedNode("p2"), DF.namedNode("p3"));

      const rules: RuleSet = [
        {
          premise: DF.namedNode("p1"),
          inference: QuadTransformMultipleVocabulary.SAME_AS,
          conclusion: DF.namedNode("c1"),
        },
        {
          premise: DF.namedNode("p2"),
          inference: QuadTransformMultipleVocabulary.SAME_AS,
          conclusion: DF.namedNode("c2"),
        },
        {
          premise: DF.namedNode("p3"),
          inference: QuadTransformMultipleVocabulary.SAME_AS,
          conclusion: DF.namedNode("c3"),
        },
      ];

      const resp = QuadTransformMultipleVocabulary.transfromQuadFromRuleSet(quad, rules);

      expect(resp).toEqual(expectedQuad);
    });

    it("should produce a quads given one matching rules", () => {
      const quad: RDF.BaseQuad = DF.quad(
        DF.namedNode("c1"),
        DF.namedNode("z"),
        DF.namedNode("q")
      );
      const expectedQuad = DF.quad(DF.namedNode("p1"), DF.namedNode("z"), DF.namedNode("q"));

      const rules: RuleSet = [
        {
          premise: DF.namedNode("p1"),
          inference: QuadTransformMultipleVocabulary.SAME_AS,
          conclusion: DF.namedNode("c1"),
        },
        {
          premise: DF.namedNode("p2"),
          inference: QuadTransformMultipleVocabulary.SAME_AS,
          conclusion: DF.namedNode("c2"),
        },
        {
          premise: DF.namedNode("p3"),
          inference: QuadTransformMultipleVocabulary.SAME_AS,
          conclusion: DF.namedNode("c3"),
        },
      ];

      const resp = QuadTransformMultipleVocabulary.transfromQuadFromRuleSet(quad, rules);

      expect(resp).toEqual(expectedQuad);
    });

    it("should produce a quads given no matching rules", () => {
      const quad: RDF.BaseQuad = DF.quad(
        DF.namedNode("w"),
        DF.namedNode("z"),
        DF.namedNode("q")
      );
      const expectedQuad = DF.quad(DF.namedNode("w"), DF.namedNode("z"), DF.namedNode("q"));

      const rules: RuleSet = [
        {
          premise: DF.namedNode("p1"),
          inference: QuadTransformMultipleVocabulary.SAME_AS,
          conclusion: DF.namedNode("c1"),
        },
        {
          premise: DF.namedNode("p2"),
          inference: QuadTransformMultipleVocabulary.SAME_AS,
          conclusion: DF.namedNode("c2"),
        },
        {
          premise: DF.namedNode("p3"),
          inference: QuadTransformMultipleVocabulary.SAME_AS,
          conclusion: DF.namedNode("c3"),
        },
      ];

      const resp = QuadTransformMultipleVocabulary.transfromQuadFromRuleSet(quad, rules);

      expect(resp).toEqual(expectedQuad);
    });

    it("should apply the first rule given a transitive rules", () => {
      const quad: RDF.BaseQuad = DF.quad(
        DF.namedNode("c1"),
        DF.namedNode("c2"),
        DF.namedNode("c3")
      );
      const expectedQuad = DF.quad(DF.namedNode("p1"), DF.namedNode("c1"), DF.namedNode("p3"));
      
      const rules: RuleSet = [
        {
          premise: DF.namedNode("p1"),
          inference: QuadTransformMultipleVocabulary.SAME_AS,
          conclusion: DF.namedNode("c1"),
        },
        {
          premise: DF.namedNode("c1"),
          inference: QuadTransformMultipleVocabulary.SAME_AS,
          conclusion: DF.namedNode("c2"),
        },
        {
          premise: DF.namedNode("p3"),
          inference: QuadTransformMultipleVocabulary.SAME_AS,
          conclusion: DF.namedNode("c3"),
        },
      ];

      const resp = QuadTransformMultipleVocabulary.transfromQuadFromRuleSet(quad, rules);

      expect(resp).toEqual(expectedQuad);
    });
  });

  describe("getRuleSet", ()=>{

  });
});

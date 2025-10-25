import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import type {
  IRule,
  IQuadTransformMultipleVocabulariesOptions,
  RuleSet,
} from '../../../lib/transform/QuadTransformMultipleVocabulary';
import {
  Operator,
  QuadTransformMultipleVocabulary,
} from '../../../lib/transform/QuadTransformMultipleVocabulary';

const DF = new DataFactory<RDF.BaseQuad>();

describe(QuadTransformMultipleVocabulary.name, () => {
  describe(QuadTransformMultipleVocabulary.transformTerm.name, () => {
    it('should return the premise of a rule given the term targeted is the conclusion', () => {
      const term = DF.namedNode('c');
      const rule: IRule = {
        premise: DF.namedNode('p'),
        inference: DF.namedNode(Operator.SAME_AS),
        conclusion: DF.namedNode('c'),
      };
      const resp = QuadTransformMultipleVocabulary.transformTerm(term, rule);
      expect(resp).toEqual(DF.namedNode('p'));
    });

    it('should return the term given a rule not supported', () => {
      const term = DF.namedNode('c');
      const rule: IRule = {
        premise: DF.namedNode('p'),
        inference: DF.namedNode('i'),
        conclusion: DF.namedNode('c'),
      };
      const resp = QuadTransformMultipleVocabulary.transformTerm(term, rule);
      expect(resp).toEqual(term);
    });

    it('should return the term given an unrelated term', () => {
      const term = DF.namedNode('z');
      const rule: IRule = {
        premise: DF.namedNode('p'),
        inference: DF.namedNode(Operator.SAME_AS),
        conclusion: DF.namedNode('c'),
      };
      const resp = QuadTransformMultipleVocabulary.transformTerm(term, rule);
      expect(resp).toEqual(term);
    });
  });

  describe(QuadTransformMultipleVocabulary.transformQuad.name, () => {
    it('should transform the quad given a fully matching rule', () => {
      const quad: RDF.BaseQuad = DF.quad(
        DF.namedNode('c'),
        DF.namedNode('c'),
        DF.namedNode('c'),
      );
      const expectedQuad = DF.quad(
        DF.namedNode('p'),
        DF.namedNode('p'),
        DF.namedNode('p'),
      );
      const rule: IRule = {
        premise: DF.namedNode('p'),
        inference: DF.namedNode(Operator.SAME_AS),
        conclusion: DF.namedNode('c'),
      };

      const resp = QuadTransformMultipleVocabulary.transformQuad(quad, rule);

      expect(resp).toEqual(expectedQuad);
    });

    it('should transform the quad given a partially matching rule', () => {
      const quad: RDF.BaseQuad = DF.quad(
        DF.namedNode('z'),
        DF.namedNode('p'),
        DF.namedNode('c'),
      );
      const expectedQuad = DF.quad(
        DF.namedNode('z'),
        DF.namedNode('p'),
        DF.namedNode('p'),
      );
      const rule: IRule = {
        premise: DF.namedNode('p'),
        inference: DF.namedNode(Operator.SAME_AS),
        conclusion: DF.namedNode('c'),
      };

      const resp = QuadTransformMultipleVocabulary.transformQuad(quad, rule);

      expect(resp).toEqual(expectedQuad);
    });

    it('should transform the quad given a non-matching rule', () => {
      const quad: RDF.BaseQuad = DF.quad(
        DF.namedNode('z'),
        DF.namedNode('p'),
        DF.namedNode('q'),
      );
      const rule: IRule = {
        premise: DF.namedNode('p'),
        inference: DF.namedNode(Operator.SAME_AS),
        conclusion: DF.namedNode('c'),
      };

      const resp = QuadTransformMultipleVocabulary.transformQuad(quad, rule);

      expect(resp).toEqual(quad);
    });
  });

  describe(QuadTransformMultipleVocabulary.transfromQuadFromRuleSet.name, () => {
    it('should produce a quads given multiple matching rules', () => {
      const quad: RDF.BaseQuad = DF.quad(
        DF.namedNode('c1'),
        DF.namedNode('c2'),
        DF.namedNode('c3'),
      );
      const expectedQuad = DF.quad(
        DF.namedNode('p1'),
        DF.namedNode('p2'),
        DF.namedNode('p3'),
      );

      const rules: RuleSet = [
        {
          premise: DF.namedNode('p1'),
          inference: DF.namedNode(Operator.SAME_AS),
          conclusion: DF.namedNode('c1'),
        },
        {
          premise: DF.namedNode('p2'),
          inference: DF.namedNode(Operator.SAME_AS),
          conclusion: DF.namedNode('c2'),
        },
        {
          premise: DF.namedNode('p3'),
          inference: DF.namedNode(Operator.SAME_AS),
          conclusion: DF.namedNode('c3'),
        },
      ];

      const resp = QuadTransformMultipleVocabulary.transfromQuadFromRuleSet(
        quad,
        rules,
      );

      expect(resp).toEqual(expectedQuad);
    });

    it('should produce a quads given one matching rules', () => {
      const quad: RDF.BaseQuad = DF.quad(
        DF.namedNode('c1'),
        DF.namedNode('z'),
        DF.namedNode('q'),
      );
      const expectedQuad = DF.quad(
        DF.namedNode('p1'),
        DF.namedNode('z'),
        DF.namedNode('q'),
      );

      const rules: RuleSet = [
        {
          premise: DF.namedNode('p1'),
          inference: DF.namedNode(Operator.SAME_AS),
          conclusion: DF.namedNode('c1'),
        },
        {
          premise: DF.namedNode('p2'),
          inference: DF.namedNode(Operator.SAME_AS),
          conclusion: DF.namedNode('c2'),
        },
        {
          premise: DF.namedNode('p3'),
          inference: DF.namedNode(Operator.SAME_AS),
          conclusion: DF.namedNode('c3'),
        },
      ];

      const resp = QuadTransformMultipleVocabulary.transfromQuadFromRuleSet(
        quad,
        rules,
      );

      expect(resp).toEqual(expectedQuad);
    });

    it('should produce a quads given no matching rules', () => {
      const quad: RDF.BaseQuad = DF.quad(
        DF.namedNode('w'),
        DF.namedNode('z'),
        DF.namedNode('q'),
      );
      const expectedQuad = DF.quad(
        DF.namedNode('w'),
        DF.namedNode('z'),
        DF.namedNode('q'),
      );

      const rules: RuleSet = [
        {
          premise: DF.namedNode('p1'),
          inference: DF.namedNode(Operator.SAME_AS),
          conclusion: DF.namedNode('c1'),
        },
        {
          premise: DF.namedNode('p2'),
          inference: DF.namedNode(Operator.SAME_AS),
          conclusion: DF.namedNode('c2'),
        },
        {
          premise: DF.namedNode('p3'),
          inference: DF.namedNode(Operator.SAME_AS),
          conclusion: DF.namedNode('c3'),
        },
      ];

      const resp = QuadTransformMultipleVocabulary.transfromQuadFromRuleSet(
        quad,
        rules,
      );

      expect(resp).toEqual(expectedQuad);
    });

    it('should apply the first rule given a transitive rules', () => {
      const quad: RDF.BaseQuad = DF.quad(
        DF.namedNode('c1'),
        DF.namedNode('c2'),
        DF.namedNode('c3'),
      );
      const expectedQuad = DF.quad(
        DF.namedNode('p1'),
        DF.namedNode('c1'),
        DF.namedNode('p3'),
      );

      const rules: RuleSet = [
        {
          premise: DF.namedNode('p1'),
          inference: DF.namedNode(Operator.SAME_AS),
          conclusion: DF.namedNode('c1'),
        },
        {
          premise: DF.namedNode('c1'),
          inference: DF.namedNode(Operator.SAME_AS),
          conclusion: DF.namedNode('c2'),
        },
        {
          premise: DF.namedNode('p3'),
          inference: DF.namedNode(Operator.SAME_AS),
          conclusion: DF.namedNode('c3'),
        },
      ];

      const resp = QuadTransformMultipleVocabulary.transfromQuadFromRuleSet(
        quad,
        rules,
      );

      expect(resp).toEqual(expectedQuad);
    });
  });

  describe('getRuleSet', () => {
    it('should get no rule given a transformer with no rule set', () => {
      const args: IQuadTransformMultipleVocabulariesOptions = {
        datasetPatterns: '^(.*\\/pods\\/[0-9]+\\/)',
        rules: [],
      };
      const quad = <RDF.Quad>(
        DF.quad(
          DF.namedNode('http://example.com/api/pods/4567/'),
          DF.blankNode(),
          DF.blankNode(),
        )
      );
      const transformer = new QuadTransformMultipleVocabulary(args);
      const ruleSet = transformer.getRuleSet(quad);
      expect(ruleSet).toBeUndefined();
    });

    it('should get rules given a transformer with rules and a matching subject', () => {
      const args: IQuadTransformMultipleVocabulariesOptions = {
        datasetPatterns: '^(.*\\/pods\\/[0-9]+\\/)',
        rules: [
          [
            {
              premise: DF.namedNode('p11'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c11'),
            },
            {
              premise: DF.namedNode('p12'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c12'),
            },
          ],
          [
            {
              premise: DF.namedNode('p21'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c21'),
            },
            {
              premise: DF.namedNode('p22'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c22'),
            },
          ],
        ],
      };
      const quad = <RDF.Quad>(
        DF.quad(
          DF.namedNode('http://example.com/api/pods/4567/barfoo'),
          DF.blankNode(),
          DF.blankNode(),
        )
      );
      const expectedIndex = QuadTransformMultipleVocabulary.stringToNumberHash('http://example.com/api/pods/4567/') % args.rules.length;
      const expectedRuleSet = args.rules[expectedIndex];
      const transformer = new QuadTransformMultipleVocabulary(args);
      const ruleSet = transformer.getRuleSet(quad);
      expect(ruleSet).toEqual(expectedRuleSet);
    });

    it('should get rules given a transformer with rules and a matching object', () => {
      const args: IQuadTransformMultipleVocabulariesOptions = {
        datasetPatterns: '^(.*\\/pods\\/[0-9]+\\/)',
        rules: [
          [
            {
              premise: DF.namedNode('p11'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c11'),
            },
            {
              premise: DF.namedNode('p12'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c12'),
            },
          ],
          [
            {
              premise: DF.namedNode('p21'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c21'),
            },
            {
              premise: DF.namedNode('p22'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c22'),
            },
          ],
        ],
      };
      const quad = <RDF.Quad>(
        DF.quad(
          DF.blankNode(),
          DF.blankNode(),
          DF.namedNode('http://example.com/api/pods/4567/barfoo'),
        )
      );
      const expectedIndex = QuadTransformMultipleVocabulary.stringToNumberHash('http://example.com/api/pods/4567/') % args.rules.length;
      const expectedRuleSet = args.rules[expectedIndex];
      const transformer = new QuadTransformMultipleVocabulary(args);
      const ruleSet = transformer.getRuleSet(quad);
      expect(ruleSet).toEqual(expectedRuleSet);
    });

    it('should not get rules given a transformer with rules and a matching predicate', () => {
      const args: IQuadTransformMultipleVocabulariesOptions = {
        datasetPatterns: '^(.*\\/pods\\/[0-9]+\\/)',
        rules: [
          [
            {
              premise: DF.namedNode('p11'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c11'),
            },
            {
              premise: DF.namedNode('p12'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c12'),
            },
          ],
          [
            {
              premise: DF.namedNode('p21'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c21'),
            },
            {
              premise: DF.namedNode('p22'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c22'),
            },
          ],
        ],
      };
      const quad = <RDF.Quad>(
        DF.quad(
          DF.blankNode(),
          DF.namedNode('http://example.com/api/pods/4567/barfoo'),
          DF.blankNode(),
        )
      );

      const transformer = new QuadTransformMultipleVocabulary(args);
      const ruleSet = transformer.getRuleSet(quad);
      expect(ruleSet).toBeUndefined();
    });
  });

  describe('constructor', () => {
    it('should throw given an inference that is not supported', () => {
      const args: IQuadTransformMultipleVocabulariesOptions = {
        datasetPatterns: '^(.*\\/pods\\/[0-9]+\\/)',
        rules: [
          [
            {
              premise: DF.namedNode('p11'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c11'),
            },
            {
              premise: DF.namedNode('p12'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c12'),
            },
          ],
          [
            {
              premise: DF.namedNode('p21'),
              inference: DF.blankNode(),
              conclusion: DF.namedNode('c21'),
            },
            {
              premise: DF.namedNode('p22'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c22'),
            },
          ],
        ],
      };
      expect(() => new QuadTransformMultipleVocabulary(args)).toThrow();
    });

    it('should construct', () => {
      const args: IQuadTransformMultipleVocabulariesOptions = {
        datasetPatterns: '^(.*\\/pods\\/[0-9]+\\/)',
        rules: [
          [
            {
              premise: DF.namedNode('p11'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c11'),
            },
            {
              premise: DF.namedNode('p12'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c12'),
            },
          ],
          [
            {
              premise: DF.namedNode('p21'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c21'),
            },
            {
              premise: DF.namedNode('p22'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c22'),
            },
          ],
        ],
      };
      const transformer = new QuadTransformMultipleVocabulary(args);

      expect(transformer.rules).toEqual(args.rules);
      expect(transformer.datasetPatterns.source).toBe(args.datasetPatterns);
    });
  });

  describe('transform', () => {
    it('should return the same quad given no rule set is associated with the dataset of the quad', () => {
      const args: IQuadTransformMultipleVocabulariesOptions = {
        datasetPatterns: '^(.*\\/pods\\/[0-9]+\\/)',
        rules: [
          [
            {
              premise: DF.namedNode('p11'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c11'),
            },
            {
              premise: DF.namedNode('p12'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c12'),
            },
          ],
          [
            {
              premise: DF.namedNode('p21'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c21'),
            },
            {
              premise: DF.namedNode('p22'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c22'),
            },
          ],
        ],
      };
      const quad = <RDF.Quad>(
        DF.quad(
          DF.blankNode(),
          DF.namedNode('http://example.com/api/pods/4567/barfoo'),
          DF.blankNode(),
        )
      );

      const transformer = new QuadTransformMultipleVocabulary(args);

      const resp = transformer.transform(quad);

      expect(resp).toStrictEqual([ quad ]);
    });

    it('should return the same quad given a rule not related to the quad', () => {
      const args: IQuadTransformMultipleVocabulariesOptions = {
        datasetPatterns: '^(.*\\/pods\\/[0-9]+\\/)',
        rules: [
          [
            {
              premise: DF.namedNode('p11'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c11'),
            },
            {
              premise: DF.namedNode('p12'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c12'),
            },
          ],
          [
            {
              premise: DF.namedNode('p21'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c21'),
            },
            {
              premise: DF.namedNode('p22'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c22'),
            },
          ],
        ],
      };
      const quad = <RDF.Quad>(
        DF.quad(
          DF.namedNode('http://example.com/api/pods/4567/barfoo'),
          DF.blankNode(),
          DF.namedNode('bar'),
        )
      );

      const transformer = new QuadTransformMultipleVocabulary(args);

      const resp = transformer.transform(quad);

      expect(resp).toStrictEqual([ quad ]);
    });

    it('should return the transformed quad given a rule related to the quad', () => {
      const args: IQuadTransformMultipleVocabulariesOptions = {
        datasetPatterns: '^(.*\\/pods\\/[0-9]+\\/)',
        rules: [
          [
            {
              premise: DF.namedNode('p21'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c21'),
            },
            {
              premise: DF.namedNode('p22'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c22'),
            },
          ],
          [
            {
              premise: DF.namedNode('p11'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c11'),
            },
            {
              premise: DF.namedNode('p12'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('predicate'),
            },
          ],
        ],
      };
      const quad = <RDF.Quad>(
        DF.quad(
          DF.namedNode('http://example.com/api/pods/4567/barfoo'),
          DF.namedNode('predicate'),
          DF.namedNode('bar'),
        )
      );

      const expectedQuad = <RDF.Quad>(
        DF.quad(
          DF.namedNode('http://example.com/api/pods/4567/barfoo'),
          DF.namedNode('p12'),
          DF.namedNode('bar'),
        )
      );

      const transformer = new QuadTransformMultipleVocabulary(args);

      const resp = transformer.transform(quad);

      expect(resp).toStrictEqual([ expectedQuad ]);
    });

    it('should return the transformed quad given a rule related to the quad given a second pass', () => {
      const args: IQuadTransformMultipleVocabulariesOptions = {
        datasetPatterns: '^(.*\\/pods\\/[0-9]+\\/)',
        rules: [
          [
            {
              premise: DF.namedNode('p21'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c21'),
            },
            {
              premise: DF.namedNode('p22'),
              inference: DF.namedNode(Operator.EQUIVALENT_CLASS),
              conclusion: DF.namedNode('c22'),
            },
          ],
          [
            {
              premise: DF.namedNode('p11'),
              inference: DF.namedNode(Operator.SAME_AS),
              conclusion: DF.namedNode('c11'),
            },
            {
              premise: DF.namedNode('p12'),
              inference: DF.namedNode(Operator.NARROW_MATCH),
              conclusion: DF.namedNode('predicate'),
            },
          ],
        ],
      };
      const quad = <RDF.Quad>(
        DF.quad(
          DF.namedNode('http://example.com/api/pods/4567/barfoo'),
          DF.namedNode('predicate'),
          DF.namedNode('bar'),
        )
      );

      const expectedQuad = <RDF.Quad>(
        DF.quad(
          DF.namedNode('http://example.com/api/pods/4567/barfoo'),
          DF.namedNode('p12'),
          DF.namedNode('bar'),
        )
      );

      const transformer = new QuadTransformMultipleVocabulary(args);

      const resp = transformer.transform(quad);

      expect(resp).toStrictEqual([ expectedQuad ]);

      const quad2 = <RDF.Quad>(
        DF.quad(
          DF.namedNode('c11'),
          DF.namedNode('predicate'),
          DF.namedNode('http://example.com/api/pods/4567/deux'),
        )
      );

      const expectedQuad2 = <RDF.Quad>(
        DF.quad(
          DF.namedNode('p11'),
          DF.namedNode('p12'),
          DF.namedNode('http://example.com/api/pods/4567/deux'),
        )
      );
      const resp2 = transformer.transform(quad2);

      expect(resp2).toStrictEqual([ expectedQuad2 ]);
    });
  });
});

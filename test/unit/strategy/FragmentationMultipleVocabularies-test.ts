import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import {
  FragmentationMultipleVocabularies,
} from '../../../lib/strategy/FragmentationMultipleVocabularies';
import type {
  RuleSet,
} from '../../../lib/transform/QuadTransformMultipleVocabulary';
import {
  QuadTransformMultipleVocabulary,
} from '../../../lib/transform/QuadTransformMultipleVocabulary';
import 'jest-rdf';

const DF = new DataFactory<RDF.Quad>();

const streamifyArray = require('streamify-array');

describe(FragmentationMultipleVocabularies.name, () => {
  describe('generateTriplesFromRules', () => {
    const A_DATASET = 'foo';
    const A_RULE_PATH = 'here.ttl';
    let stategy: FragmentationMultipleVocabularies;

    beforeEach(() => {
      stategy = new FragmentationMultipleVocabularies({
        datasetPatterns: '',
        rules: [],
        rulePath: A_RULE_PATH,
      });
    });

    it('should return no quads given an empty rule set', () => {
      const ruleSet: RuleSet = [];
      const resp = stategy.generateTriplesFromRules(ruleSet, A_DATASET);

      expect(resp).toEqual([]);
    });

    it('should return the quads given a rule set', () => {
      const ruleSet: RuleSet = [
        {
          premise: DF.namedNode('p1'),
          inference: DF.namedNode('i1'),
          conclusion: DF.namedNode('c1'),
        },
        {
          premise: DF.namedNode('p2'),
          inference: DF.namedNode('i2'),
          conclusion: DF.namedNode('c2'),
        },
        {
          premise: DF.namedNode('p3'),
          inference: DF.namedNode('i3'),
          conclusion: DF.namedNode('c3'),
        },
      ];
      const documentIri = DF.namedNode(`${A_DATASET}/${A_RULE_PATH}`);
      const firstRuleBN = DF.blankNode();
      const secondRuleBN = DF.blankNode();
      const thirdRuleBN = DF.blankNode();

      const expectedTriples = [
        DF.quad(
          documentIri,
          FragmentationMultipleVocabularies.RDF_TYPE_NODE,
          FragmentationMultipleVocabularies.RULE_SET_CLASS,
        ),
        DF.quad(
          documentIri,
          FragmentationMultipleVocabularies.RULE_SET_SUBWEB,
          DF.literal(`"${A_DATASET}{+path}"`),
        ),

        DF.quad(
          documentIri,
          FragmentationMultipleVocabularies.RULE_SET_RULE,
          firstRuleBN,
        ),
        DF.quad(
          documentIri,
          FragmentationMultipleVocabularies.RULE_SET_RULE,
          secondRuleBN,
        ),
        DF.quad(
          documentIri,
          FragmentationMultipleVocabularies.RULE_SET_RULE,
          thirdRuleBN,
        ),

        DF.quad(
          firstRuleBN,
          FragmentationMultipleVocabularies.RULE_SET_PREMISE,
          DF.namedNode('p1'),
        ),
        DF.quad(
          firstRuleBN,
          FragmentationMultipleVocabularies.RULE_SET_INFERENCE,
          DF.namedNode('i1'),
        ),
        DF.quad(
          firstRuleBN,
          FragmentationMultipleVocabularies.RULE_SET_CONCLUSION,
          DF.namedNode('c1'),
        ),

        DF.quad(
          secondRuleBN,
          FragmentationMultipleVocabularies.RULE_SET_PREMISE,
          DF.namedNode('p2'),
        ),
        DF.quad(
          secondRuleBN,
          FragmentationMultipleVocabularies.RULE_SET_INFERENCE,
          DF.namedNode('i2'),
        ),
        DF.quad(
          secondRuleBN,
          FragmentationMultipleVocabularies.RULE_SET_CONCLUSION,
          DF.namedNode('c2'),
        ),

        DF.quad(
          thirdRuleBN,
          FragmentationMultipleVocabularies.RULE_SET_PREMISE,
          DF.namedNode('p3'),
        ),
        DF.quad(
          thirdRuleBN,
          FragmentationMultipleVocabularies.RULE_SET_INFERENCE,
          DF.namedNode('i3'),
        ),
        DF.quad(
          thirdRuleBN,
          FragmentationMultipleVocabularies.RULE_SET_CONCLUSION,
          DF.namedNode('c3'),
        ),
      ];
      const resp = stategy.generateTriplesFromRules(ruleSet, A_DATASET);

      expect(resp).toBeRdfIsomorphic(expectedTriples);
    });
  });

  describe('fragment', () => {
    let stategy: FragmentationMultipleVocabularies;
    const A_RULE_PATH = 'here.ttl';
    let sink: any;
    const ruleSet: RuleSet[] = [
      [
        {
          premise: DF.namedNode('p11'),
          inference: QuadTransformMultipleVocabulary.SAME_AS,
          conclusion: DF.namedNode('c11'),
        },
        {
          premise: DF.namedNode('p12'),
          inference: QuadTransformMultipleVocabulary.SAME_AS,
          conclusion: DF.namedNode('c12'),
        },
        {
          premise: DF.namedNode('p13'),
          inference: QuadTransformMultipleVocabulary.SAME_AS,
          conclusion: DF.namedNode('c13'),
        },
      ],
      [
        {
          premise: DF.namedNode('p21'),
          inference: QuadTransformMultipleVocabulary.SAME_AS,
          conclusion: DF.namedNode('c21'),
        },
        {
          premise: DF.namedNode('p22'),
          inference: QuadTransformMultipleVocabulary.SAME_AS,
          conclusion: DF.namedNode('c22'),
        },
        {
          premise: DF.namedNode('p23'),
          inference: QuadTransformMultipleVocabulary.SAME_AS,
          conclusion: DF.namedNode('c23'),
        },
      ],
      [
        {
          premise: DF.namedNode('p31'),
          inference: QuadTransformMultipleVocabulary.SAME_AS,
          conclusion: DF.namedNode('c31'),
        },
        {
          premise: DF.namedNode('p32'),
          inference: QuadTransformMultipleVocabulary.SAME_AS,
          conclusion: DF.namedNode('c32'),
        },
        {
          premise: DF.namedNode('p33'),
          inference: QuadTransformMultipleVocabulary.SAME_AS,
          conclusion: DF.namedNode('c33'),
        },
      ],
    ];

    beforeEach(() => {
      stategy = new FragmentationMultipleVocabularies({
        datasetPatterns: '^(.*\\/pods\\/[0-9]+\\/)',
        rules: ruleSet,
        rulePath: A_RULE_PATH,
      });
      sink = {
        push: jest.fn(),
      };
    });

    it('should not create a rule set given a quad not related to a dataset', async() => {
      const quad = DF.quad(
        DF.namedNode('bar'),
        DF.namedNode('foo'),
        DF.namedNode('boo'),
      );
      await stategy.fragment(streamifyArray([ quad ]), sink);
      expect(sink.push).toHaveBeenCalledTimes(0);
    });

    it('should add a rule file given a quad matching a dataset', async() => {
      const quad = DF.quad(
        DF.namedNode('http://example.com/api/pods/4567/barfoo'),
        DF.namedNode('foo'),
        DF.namedNode('boo'),
      );
      await stategy.fragment(streamifyArray([ quad ]), sink);
      // There are 3 rules by set and we need to add triples 4 triples by rules and 2 triples to declare the rule set
      expect(sink.push).toHaveBeenCalledTimes(4 * 3 + 2);
      for (let i = 1; i !== 4 + 3 + 2; i++) {
        expect(sink.push).toHaveBeenNthCalledWith(i, `http://example.com/api/pods/4567/${A_RULE_PATH}`, expect.anything());
      }
    });

    it('should not add two time a rule file', async() => {
      const quads = [
        DF.quad(
          DF.namedNode('http://example.com/api/pods/4567/barfoo'),
          DF.namedNode('foo'),
          DF.namedNode('boo'),
        ),
        DF.quad(
          DF.namedNode('boo'),
          DF.namedNode('foo'),
          DF.namedNode('http://example.com/api/pods/4567/barfoo/bla'),
        ),

      ];
      await stategy.fragment(streamifyArray(quads), sink);
      // There are 3 rules by set and we need to add triples 4 triples by rules and 2 triples to declare the rule set
      expect(sink.push).toHaveBeenCalledTimes(4 * 3 + 2);
      for (let i = 1; i !== 4 + 3 + 2; i++) {
        expect(sink.push).toHaveBeenNthCalledWith(i, `http://example.com/api/pods/4567/${A_RULE_PATH}`, expect.anything());
      }
    });

    it('should handle unrelated triples', async() => {
      const quads = [
        DF.quad(
          DF.namedNode('http://example.com/api/pods/4567/barfoo'),
          DF.namedNode('foo'),
          DF.namedNode('boo'),
        ),
        DF.quad(
          DF.namedNode('bar'),
          DF.namedNode('foo'),
          DF.namedNode('boo'),
        ),
        DF.quad(
          DF.namedNode('boo'),
          DF.namedNode('foo'),
          DF.namedNode('http://example.com/api/pods/4566/barfoo/bla'),
        ),

      ];
      await stategy.fragment(streamifyArray(quads), sink);
      // There are 3 rules by set and we need to add triples 4 triples by rules and 2 triples to declare the rule set
      expect(sink.push).toHaveBeenCalledTimes((4 * 3 + 2) * 2);
      for (let i = 1; i !== 4 + 3 + 2; i++) {
        expect(sink.push).toHaveBeenNthCalledWith(i, `http://example.com/api/pods/4567/${A_RULE_PATH}`, expect.anything());
      }
      for (let i = 4 * 3 + 2 + 1; i !== (4 * 3 + 2) * 2; i++) {
        expect(sink.push).toHaveBeenNthCalledWith(i, `http://example.com/api/pods/4566/${A_RULE_PATH}`, expect.anything());
      }
    });
  });
});

import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import type { IQuadSink } from '../io/IQuadSink';
import type {
  IQuadTransformMultipleVocabulariesOptions,
  RuleSet,
} from '../transform/QuadTransformMultipleVocabulary';
import {
  QuadTransformMultipleVocabulary,
} from '../transform/QuadTransformMultipleVocabulary';
import { FragmentationStrategyStreamAdapter } from './FragmentationStrategyStreamAdapter';

const DF = new DataFactory<RDF.Quad>();

export class FragmentationMultipleVocabularies extends FragmentationStrategyStreamAdapter {
  public readonly transformer: QuadTransformMultipleVocabulary;

  public readonly dataSetHandled: Set<string> = new Set();
  public readonly rulePath: string;
  public static readonly RDF_TYPE_NODE = DF.namedNode(
    'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
  );

  public static readonly RULE_SET_PREFIX = 'https://exemple.com#';
  public static readonly RULE_SET_CLASS = DF.namedNode(`${this.RULE_SET_PREFIX}RuleSet`);
  public static readonly RULE_SET_RULE = DF.namedNode(`${this.RULE_SET_PREFIX}rule`);
  public static readonly RULE_SET_LOCATOR_NODE = DF.namedNode(
    `${this.RULE_SET_PREFIX}ruleSetLocation`,
  );

  public static readonly RULE_SET_PREMISE = DF.namedNode(
    `${this.RULE_SET_PREFIX}premise`,
  );

  public static readonly RULE_SET_INFERENCE = DF.namedNode(
    `${this.RULE_SET_PREFIX}inference`,
  );

  public static readonly RULE_SET_CONCLUSION = DF.namedNode(
    `${this.RULE_SET_PREFIX}conclusion`,
  );

  public static readonly RULE_SET_SUBWEB = DF.namedNode(
    `${this.RULE_SET_PREFIX}subweb`,
  );

  public constructor(args: IFragmentationMultipleVocabulariesArgs) {
    super();
    this.transformer = new QuadTransformMultipleVocabulary({ ...args });
    this.rulePath = args.rulePath;
  }

  protected async handleQuad(
    quad: RDF.Quad,
    quadSink: IQuadSink,
  ): Promise<void> {
    const ruleSet = this.transformer.getRuleSet(quad);
    if (ruleSet === undefined) {
      return;
    }
    // If we are getting a rule set then it mean that we have a dataset
    const dataset = this.transformer.getMatchingDataset(quad)!;
    if (this.dataSetHandled.has(dataset)) {
      return;
    }
    this.dataSetHandled.add(dataset);
    const ruleSetQuads = this.generateTriplesFromRules(ruleSet, dataset);
    for (const quad of ruleSetQuads) {
      await quadSink.push(`${dataset}${this.rulePath}`, quad);
    }
  }

  /**
   * Generate KG of the rule set that translate the pod from the original vocabulary to the projected one.
   * @param {RuleSet} rules
   * @param {string} dataset
   * @returns {RDF.Quad[]}
   */
  public generateTriplesFromRules(rules: RuleSet, dataset: string): RDF.Quad[] {
    if (rules.length === 0) {
      return [];
    }
    const documentIri = DF.namedNode(`${dataset}/${this.rulePath}`);

    const resp: RDF.Quad[] = [
      DF.quad(documentIri, FragmentationMultipleVocabularies.RDF_TYPE_NODE, FragmentationMultipleVocabularies.RULE_SET_CLASS),
      DF.quad(documentIri, FragmentationMultipleVocabularies.RULE_SET_SUBWEB, DF.literal(`${dataset}{+path}`)),
    ];

    for (const rule of rules) {
      const ruleIri = DF.blankNode();
      const declarationQuad = DF.quad(documentIri, FragmentationMultipleVocabularies.RULE_SET_RULE, ruleIri);

      const premiseQuad = DF.quad(ruleIri, FragmentationMultipleVocabularies.RULE_SET_PREMISE, rule.premise);
      const inferenceQuad = DF.quad(ruleIri, FragmentationMultipleVocabularies.RULE_SET_INFERENCE, rule.inference);
      const conclusionQuad = DF.quad(ruleIri, FragmentationMultipleVocabularies.RULE_SET_CONCLUSION, rule.conclusion);

      resp.push(declarationQuad, premiseQuad, inferenceQuad, conclusionQuad);
    }

    return resp;
  }
}

/**
 * The argument of the component
 */
export interface IFragmentationMultipleVocabulariesArgs extends IQuadTransformMultipleVocabulariesOptions{

  /**
   * Relative path of the rule set
   */
  rulePath: string;
}

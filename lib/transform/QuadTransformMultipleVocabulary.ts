import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import type { IQuadTransformer } from './IQuadTransformer';

const DF = new DataFactory<RDF.BaseQuad>();
/**
 * A quad transformer that generate quads into another vocabulary by data sources
 */
export class QuadTransformMultipleVocabulary implements IQuadTransformer {
  public readonly datasetPatterns: RegExp;
  public readonly rules: RuleSet[];
  public readonly dataSetRuleAssoc: Map<string, RuleSet> = new Map();
  public static readonly SAME_AS = DF.namedNode(
    'http://www.w3.org/2002/07/owl#sameAs',
  );

  public constructor(args: IQuadTransformMultipleVocabulariesOptions) {
    this.datasetPatterns = new RegExp(args.datasetPatterns, 'u');
    const rules = args.rules;
    for (const ruleSet of rules) {
      for (const rule of ruleSet) {
        if (QuadTransformMultipleVocabulary.SAME_AS.value !== rule.inference.value) {
          throw new Error(
            `${rule.inference.value} is not a suported inference`,
          );
        }
        rule.premise = DF.fromTerm(<RDF.Term>rule.premise);
        rule.inference = DF.fromTerm(<RDF.Term>rule.inference);
        rule.conclusion = DF.fromTerm(<RDF.Term>rule.conclusion);
      }
    }
    this.rules = <RuleSet[]>rules;
  }

  public transform(quad: RDF.Quad): RDF.Quad[] {
    const ruleSet = this.getRuleSet(quad);
    if (ruleSet === undefined) {
      return [ quad ];
    }
    // We cast because nothing stop a user to produce base quad instead of quads
    return [
      <RDF.Quad>(
        QuadTransformMultipleVocabulary.transfromQuadFromRuleSet(quad, ruleSet)
      ),
    ];
  }

  /**
   * From a quad get the matching dataset
   * @param {RDF.Quad} quad
   * @returns {string | undefined} the matching dataset
   */
  public getMatchingDataset(quad: RDF.Quad): string | undefined {
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
  public getRuleSet(quad: RDF.Quad): RuleSet | undefined {
    const dataset = this.getMatchingDataset(quad);
    if (dataset === undefined) {
      return undefined;
    }

    const ruleSet = this.dataSetRuleAssoc.get(dataset);
    if (ruleSet !== undefined) {
      return ruleSet;
    }

    const datasetToNumber =
      QuadTransformMultipleVocabulary.stringToNumberHash(dataset);
    const index = datasetToNumber % this.rules.length;
    const currentRuleSet = this.rules[index];
    this.dataSetRuleAssoc.set(dataset, currentRuleSet);

    return currentRuleSet;
  }

  public static stringToNumberHash(str: string): number {
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
  public static transfromQuadFromRuleSet(
    quad: RDF.BaseQuad,
    ruleSet: RuleSet,
  ): RDF.BaseQuad {
    const resp: RDF.BaseQuad[] = [];
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
  private static mergeQuad(
    quads: RDF.BaseQuad[],
    originalQuad: RDF.BaseQuad,
  ): RDF.BaseQuad {
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
  public static transformQuad(quad: RDF.BaseQuad, rule: IRule): RDF.BaseQuad {
    const subject: RDF.Term = this.transformTerm(quad.subject, rule);
    const predicate: RDF.Term = this.transformTerm(quad.predicate, rule);
    const object: RDF.Term = this.transformTerm(quad.object, rule);

    return DF.quad(subject, predicate, object);
  }

  /**
   * Transform a RDF term based on the reverse of a rule.
   * Cannot work if the rule cannot be reverted.
   * @param {RDF.Term} term an RDF term
   * @param {IRule} rule A rule
   * @returns {RDF.Term} The premise of the rule
   */
  public static transformTerm(term: RDF.Term, rule: IRule): RDF.Term {
    if (
      rule.conclusion.equals(term) &&
      rule.inference.equals(QuadTransformMultipleVocabulary.SAME_AS)
    ) {
      return rule.premise;
    }
    return term;
  }
}

/**
 * The argument of the component
 */
export interface IQuadTransformMultipleVocabulariesOptions {
  /**
   * Regex identifying a dataset
   */
  datasetPatterns: string;
  /**
   * The sets of rules that change the vocabulary of the dataset
   */
  rules: IRuleArg[][];
}

export type RuleSet = IRule[];

export interface IRule {
  premise: RDF.NamedNode | RDF.BlankNode | RDF.Literal;
  inference: RDF.NamedNode | RDF.BlankNode | RDF.Literal;
  conclusion: RDF.NamedNode | RDF.BlankNode | RDF.Literal;
}

export interface ITerm {
  value:string;
  termType:string;
}

export interface IRuleArg{
  premise: ITerm;
  inference:ITerm;
  conclusion:ITerm
}
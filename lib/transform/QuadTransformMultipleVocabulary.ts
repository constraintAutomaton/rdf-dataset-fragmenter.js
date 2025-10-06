import type * as RDF from "@rdfjs/types";
import type { QuadTermName } from "rdf-terms";
import { mapTerms } from "rdf-terms";
import type { IQuadTransformer } from "./IQuadTransformer";
import { DataFactory } from "rdf-data-factory";

const DF = new DataFactory<RDF.Quad>;
/**
 * A quad transformer that generate quads into another vocabulary by data sources
 */
export abstract class QuadTransformMultipleVocabulary
  implements IQuadTransformer
{
  public readonly datasetPatterns: RegExp;
  public readonly ruleFilepath: string;
  public readonly rules: RuleSet[];
  public readonly dataSetRuleAssoc: Map<string, RuleSet> = new Map();
  public static readonly SAME_AS = "http://www.w3.org/2002/07/owl#sameAs";

  public constructor(args: IQuadTransformMultipleVocabularyArgs) {
    this.datasetPatterns = new RegExp(args.datasetPatterns, "u");
    this.ruleFilepath = args.ruleFilepath;
    this.rules = args.rules;
    for (const ruleSet of this.rules) {
      for (const rule of ruleSet) {
        if (rule.inference.value !== QuadTransformMultipleVocabulary.SAME_AS) {
          throw new Error(
            `${rule.inference.value} is not a suported inference`
          );
        }
      }
    }
  }

  public transform(quad: RDF.Quad): RDF.Quad[] {
    return [quad];
  }

  public generate_rule_file(): RDF.Quad[] {
    return [];
  }

  public applicable_rule_set(quad: RDF.Quad): RuleSet | undefined {
    const matches = quad.value.match(this.datasetPatterns);
    if (matches === null) {
      return undefined;
    }
    const dataset = matches[0];

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

  public static stringToNumberHash(str: string) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      hash = (hash << 5) - hash + str.charCodeAt(i);
      hash |= 0;
    }
    return hash;
  }

  public generateQuadFromRuleSet(quad: RDF.Quad, ruleSet: RuleSet): RDF.Quad[] {
    const resp: RDF.Quad[] = [];
    for (const rule of ruleSet) {
    }
    return resp;
  }

  public transformQuad(quad: RDF.Quad, rule: IRule): RDF.Quad {
    let subject: RDF.Term = this.transformTerm(quad.subject, rule);
    let predicate: RDF.Term = this.transformTerm(quad.predicate, rule);
    let object: RDF.Term = this.transformTerm(quad.predicate, rule);
    
    return DF.quad(subject, predicate, object);
  }

  public transformTerm<Q extends RDF.Term>(term: Q, rule: IRule): Q {
    if (
      rule.conclusion.equals(term) &&
      rule.inference.value === QuadTransformMultipleVocabulary.SAME_AS
    ) {
      return rule.premise;
    }
    return term;
  }
}

interface IQuadTransformMultipleVocabularyArgs {
  datasetPatterns: string;
  ruleFilepath: string;
  rules: RuleSet[];
}

type RuleSet = IRule[];

interface IRule {
  premise: RDF.Term;
  inference: RDF.Term;
  conclusion: RDF.Term;
}

import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import type { IValueModifier } from './IValueModifier';

const DF = new DataFactory();

/**
 * A value modifier that applies the given regex on the value and replaces it with the first group match.
 */
export class ValueModifierRegexReplace implements IValueModifier {
  private readonly regex: RegExp;

  public constructor(regex: string, funFlag:boolean) {
    this.regex = new RegExp(regex, 'u');

  }

  public apply(value: RDF.Term): RDF.Term {
    const matches = this.regex.exec(value.value);
    console.log(this.regex);
    console.log(value.value);
    if(matches){
      console.log("Applying the quad")
      return DF.namedNode("abc");
    }
    return value;
  }
}

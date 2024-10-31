import type * as RDF from '@rdfjs/types';
import type { QuadTermName } from 'rdf-terms';
import type { ITermTemplate } from './ITermTemplate';
import type { IValueModifier } from '../value/IValueModifier';

/**
 * A term template that returns a given quad's component.
 */
export class TermTemplateQuadComponent implements ITermTemplate {
  public readonly component: QuadTermName
  public readonly valueModifier?: IValueModifier

  public constructor(component: QuadTermName, valueModifier?: IValueModifier) {
    this.component = component;
    this.valueModifier = valueModifier;
  }

  public getTerm(quad: RDF.Quad): RDF.Term {
    if (this.valueModifier !== undefined) {
      console.log("modifying the quad")
      return this.valueModifier.apply(quad[this.component]);
    }
    return quad[this.component];
  }
}

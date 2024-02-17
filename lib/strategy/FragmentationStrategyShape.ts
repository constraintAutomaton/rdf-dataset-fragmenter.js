import { readFileSync } from 'fs';
import { readFile } from 'fs/promises';
import { join } from 'path';
import type * as RDF from '@rdfjs/types';
import * as ShexParser from '@shexjs/parser';
import { JsonLdParser } from 'jsonld-streaming-parser';
import { DataFactory } from 'rdf-data-factory';
import type { IQuadSink } from '../io/IQuadSink';
import { FragmentationStrategyStreamAdapter } from './FragmentationStrategyStreamAdapter';
import { FragmentationStrategySubject } from './FragmentationStrategySubject';

const DF = new DataFactory<RDF.Quad>();

export class FragmentationStrategyShape extends FragmentationStrategyStreamAdapter {
  private readonly relativePath?: string;
  private readonly tripleShapeTreeLocator?: boolean;
  private readonly shapeMap: Map<string, string>;

  private readonly resourceHandled: Set<string> = new Set();
  // This filter is for the case where all the resources are inside one file
  // in that case we want the shape and the index to target
  // all the subject inside the file and not generate one file by subject
  private readonly singleFileContainerHandled: Set<string> = new Set();

  public static readonly rdfTypeNode = DF.namedNode('http://www.w3.org/1999/02/22-rdf-syntax-ns#type');
  public static readonly shapeTreeNode = DF.namedNode('http://www.w3.org/ns/shapetrees#ShapeTree');
  public static readonly shapeTreeShapeNode = DF.namedNode('http://www.w3.org/ns/shapetrees#shape');
  public static readonly shapeTreeLocator = DF.namedNode('http://www.w3.org/ns/shapetrees#ShapeTreeLocator');
  public static readonly solidInstance = DF.namedNode('http://www.w3.org/ns/solid/terms#instance');
  public static readonly solidInstanceContainer = DF.namedNode('http://www.w3.org/ns/solid/terms#instanceContainer');
  public static readonly shapeTreeFileName: string = 'shapetree.nq';

  public constructor(shapeFolder: string, relativePath?: string, tripleShapeTreeLocator?: boolean) {
    super();
    this.tripleShapeTreeLocator = tripleShapeTreeLocator;
    this.relativePath = relativePath;
    this.shapeMap = this.generateShapeMap(shapeFolder);
  }

  private generateShapeMap(shapeFolder: string): Map<string, string> {
    const shapeMap: Map<string, string> = new Map();
    const config = JSON.parse(readFileSync(join(shapeFolder, 'config.json')).toString());
    const shapes = config.shapes;
    for (const [ dataType, shapeEntry ] of Object.entries(shapes)) {
      shapeMap.set(dataType, <string>shapeEntry);
    }
    return shapeMap;
  }

  protected async handleQuad(quad: RDF.Quad, quadSink: IQuadSink): Promise<void> {
    const iri = FragmentationStrategySubject.generateIri(quad, this.relativePath);
    if (!this.resourceHandled.has(iri)) {
      for (const [ resourceIndex, shapePath ] of this.shapeMap) {
        // We are in the case where the resource is not in the root of the pod
        const positionContainerResourceNotInRoot = iri.indexOf(`/${resourceIndex}/`);
        if (positionContainerResourceNotInRoot !== -1) {
          await FragmentationStrategyShape.generateShapeIndexInformation(quadSink,
            this.resourceHandled,
            iri,
            positionContainerResourceNotInRoot - 1,
            resourceIndex,
            shapePath,
            this.tripleShapeTreeLocator);
          return;
        }

        // We are in the case where the ressource is at the root of the pod
        const positionContainerResourceInRoot = iri.indexOf(resourceIndex);
        const resourceInOneFileId = `${iri.slice(0, Math.max(0, positionContainerResourceInRoot - 1))}/${resourceIndex}`;
        // We check if the resouce is in the root of the pod and we check if the file has been handled
        if (positionContainerResourceInRoot !== -1 && !this.singleFileContainerHandled.has(resourceInOneFileId)) {
          await FragmentationStrategyShape.generateShapeIndexInformation(quadSink,
            this.resourceHandled,
            iri,
            positionContainerResourceInRoot - 1,
            resourceIndex,
            shapePath,
            this.tripleShapeTreeLocator);
          this.singleFileContainerHandled.add(resourceInOneFileId);
          return;
        }
      }
    }
  }

  public static async generateShapeIndexInformation(quadSink: IQuadSink,
    resourceHandled: Set<string>,
    iri: string,
    positionContainer: number,
    resourceIndex: string,
    shapePath: string,
    tripleShapeTreeLocator?: boolean): Promise<void> {
    const podIRI = iri.slice(0, Math.max(0, positionContainer));
    const shapeTreeIRI = `${podIRI}/${FragmentationStrategyShape.shapeTreeFileName}`;
    const shapeIRI = `${podIRI}/${resourceIndex}_shape.nq`;

    const promises: Promise<void>[] = [];
    if (tripleShapeTreeLocator === true) {
      promises.push(FragmentationStrategyShape.generateShapeTreeLocator(quadSink, podIRI, shapeTreeIRI, iri));
    }
    promises.push(FragmentationStrategyShape.generateShape(quadSink, shapeIRI, shapePath));
    promises.push(FragmentationStrategyShape.generateShapetreeTriples(quadSink, shapeTreeIRI, shapeIRI, true, iri));
    await Promise.all(promises);
    resourceHandled.add(iri);
  }

  public static async generateShapeTreeLocator(quadSink: IQuadSink,
    podIRI: string,
    shapeTreeIRI: string,
    iri: string): Promise<void> {
    const shapeTreeIndicator = DF.quad(
      DF.namedNode(podIRI),
      this.shapeTreeLocator,
      DF.namedNode(shapeTreeIRI),
    );
    await quadSink.push(iri, shapeTreeIndicator);
  }

  public static async generateShapetreeTriples(quadSink: IQuadSink,
    shapeTreeIRI: string,
    shapeIRI: string,
    isNotInRootOfPod: boolean,
    contentIri: string): Promise<void> {
    const blankNode = DF.blankNode();
    const type = DF.quad(
      blankNode,
      this.rdfTypeNode,
      this.shapeTreeNode,
    );
    const shape = DF.quad(
      blankNode,
      this.shapeTreeShapeNode,
      DF.namedNode(shapeIRI),
    );
    const target = DF.quad(
      blankNode,
      isNotInRootOfPod ? this.solidInstance : this.solidInstanceContainer,
      DF.namedNode(contentIri),
    );
    await Promise.all(
      [
        quadSink.push(shapeTreeIRI, type),
        quadSink.push(shapeTreeIRI, shape),
        quadSink.push(shapeTreeIRI, target),
      ],
    );
  }

  public static async generateShape(quadSink: IQuadSink, shapeIRI: string, shapePath: string): Promise<void> {
    const shexParser = ShexParser.construct(shapeIRI);
    const shapeShexc = (await readFile(shapePath)).toString();
    const shapeJSONLD = shexParser.parse(shapeShexc);
    // The jsonLD is not valid without the context field and the library doesn't include it
    // because a ShExJ MAY contain a @context field
    // https://shex.io/shex-semantics/#shexj
    shapeJSONLD['@context'] = 'http://www.w3.org/ns/shex.jsonld';
    const stringShapeJsonLD = JSON.stringify(shapeJSONLD);

    return new Promise((resolve, reject) => {
      // Stringigy streams
      const promises: Promise<void>[] = [];
      const jsonldParser = new JsonLdParser();
      jsonldParser
        .on('data', async(quad: RDF.Quad) => {
          promises.push(quadSink.push(shapeIRI, quad));
        })
      // We ignore this because it is difficult to provide a valid Shex document that
      // would not be parsable in RDF when it has been in ShExJ

      // eslint-disable-next-line no-inline-comments
        .on('error', /* istanbul ignore next */(error: any) => {
          reject(error);
        })
        .on('end', async() => {
          await Promise.all(promises);
          resolve();
        });

      jsonldParser.write(stringShapeJsonLD);
      jsonldParser.end();
    });
  }

  protected async flush(quadSink: IQuadSink): Promise<void> {
    await super.flush(quadSink);
  }
}

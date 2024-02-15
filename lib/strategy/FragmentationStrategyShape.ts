import { FragmentationStrategyStreamAdapter } from './FragmentationStrategyStreamAdapter';
import { FragmentationStrategySubject } from './FragmentationStrategySubject';
import { readFileSync } from 'fs';
import { readFile } from 'fs/promises';
import { join } from 'path';
import type { IQuadSink } from '../io/IQuadSink';
import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import * as ShexParser from '@shexjs/parser';
import { JsonLdParser } from "jsonld-streaming-parser";

const DF = new DataFactory<RDF.Quad>();

export class FragmentationStrategyShape extends FragmentationStrategyStreamAdapter {
    private readonly relativePath?: string;
    private readonly tripleShapeTreeLocator?: boolean;
    private readonly shapeMap: Map<string, string>;

    private resourceHandled: Set<string> = new Set();

    private readonly shapeTreeFileName: string = "shapetree.nq";

    static rdfTypeNode = DF.namedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    static shapeTreeNode = DF.namedNode("http://www.w3.org/ns/shapetrees#ShapeTree");
    static shapeTreeShapeNode = DF.namedNode("http://www.w3.org/ns/shapetrees#shape");
    static shapeTreeLocator = DF.namedNode('http://www.w3.org/ns/shapetrees#ShapeTreeLocator');
    static solidInstance = DF.namedNode("http://www.w3.org/ns/solid/terms#instance");
    static solidInstanceContainer = DF.namedNode("http://www.w3.org/ns/solid/terms#instanceContainer");

    public constructor(shapeFolder: string, relativePath?: string, tripleShapeTreeLocator?: boolean) {
        super();
        this.tripleShapeTreeLocator = tripleShapeTreeLocator;
        this.relativePath = relativePath;
        this.shapeMap = this.generateShapeMap(shapeFolder);
    }

    private generateShapeMap(shapeFolder: string): Map<string, string> {
        const shapeMap: Map<string, string> = new Map();
        const config = JSON.parse(readFileSync(join(shapeFolder, 'config.json')).toString());
        const shapes = config["shapes"];
        for (const [dataType, shape] of Object.entries(shapes)) {
            shapeMap.set(dataType, <string>shape);
        }
        return shapeMap;
    }

    protected async handleQuad(quad: RDF.Quad, quadSink: IQuadSink): Promise<void> {
        const iri = FragmentationStrategySubject.generateIri(quad, this.relativePath);
        if (!this.resourceHandled.has(iri)) {
            for (const [resourceIndex, shapePath] of this.shapeMap) {
                // we are in the case where the resource is not in the root of the pod
                const positionContainerResourceNotInRoot = iri.indexOf(`/${resourceIndex}/`);
                if (positionContainerResourceNotInRoot !== -1) {
                    this.generateShapeIndexInformation(quadSink, iri, positionContainerResourceNotInRoot - 1, resourceIndex, shapePath);
                    return;
                }

                // we are in the case where the ressource is at the root of the pod
                const positionContainerResourceInRoot = iri.indexOf(resourceIndex);
                if (positionContainerResourceInRoot !== -1) {
                    this.generateShapeIndexInformation(quadSink, iri, positionContainerResourceNotInRoot - 1, resourceIndex, shapePath);
                    return;
                }
            }
        }
    }

    private generateShapeIndexInformation(quadSink: IQuadSink,
        iri: string,
        positionContainer: number,
        resourceIndex: string,
        shapePath: string) {
        const podIRI = iri.substring(0, positionContainer);
        const shapeTreeIRI = `${podIRI}/${this.shapeTreeFileName}`;
        const shapeIRI = `${podIRI}/${resourceIndex}_shape.nq`;

        if (this.tripleShapeTreeLocator) {
            FragmentationStrategyShape.generateShapeTreeLocator(quadSink, podIRI, shapeTreeIRI, iri);
        }
        FragmentationStrategyShape.generateShape(quadSink, shapeIRI, shapePath);
        FragmentationStrategyShape.generateShapetreeTriples(quadSink, shapeTreeIRI, shapeIRI, true, iri);
        this.resourceHandled.add(iri);
    }

    static generateShapeTreeLocator(quadSink: IQuadSink, podIRI: string, shapeTreeIRI: string, iri: string) {
        const shapeTreeIndicator = DF.quad(
            DF.namedNode(podIRI),
            this.shapeTreeLocator,
            DF.namedNode(shapeTreeIRI)
        );
        quadSink.push(iri, shapeTreeIndicator);
    }

    static generateShapetreeTriples(quadSink: IQuadSink, shapeTreeIRI: string, shapeIRI: string, isNotInRootOfPod: boolean, contentIri: string) {
        const blankNode = DF.blankNode();
        const type = DF.quad(
            blankNode,
            this.rdfTypeNode,
            this.shapeTreeNode
        );
        const shape = DF.quad(
            blankNode,
            this.shapeTreeShapeNode,
            DF.namedNode(shapeIRI)
        );
        const target = DF.quad(
            blankNode,
            isNotInRootOfPod ? this.solidInstance : this.solidInstanceContainer,
            DF.namedNode(contentIri)
        );
        quadSink.push(shapeTreeIRI, type);
        quadSink.push(shapeTreeIRI, shape);
        quadSink.push(shapeTreeIRI, target);
    }

    static async generateShape(quadSink: IQuadSink, shapeIRI: string, shapePath: string): Promise<void> {
        const shexParser = ShexParser.construct(shapeIRI);
        const shapeShexc = (await readFile(shapePath)).toString();
        const shapeJSONLD = shexParser.parse(shapeShexc);
        const stringShapeJsonLD = JSON.stringify(shapeJSONLD);
        
        return new Promise((resolve, reject) => {
            // stringigy streams
            const jsonldParser = new JsonLdParser();
            jsonldParser
                .on('data', (quad: RDF.Quad) => { quadSink.push(shapeIRI, quad) })
                .on('error', (error: any) => { reject(error) })
                .on('end', () => resolve());
            jsonldParser.write(stringShapeJsonLD);
            jsonldParser.end();

        });
    }

    protected async flush(quadSink: IQuadSink): Promise<void> {
        await super.flush(quadSink);
    }
}
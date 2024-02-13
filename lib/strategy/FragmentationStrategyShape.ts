import { FragmentationStrategyStreamAdapter } from './FragmentationStrategyStreamAdapter';
import { FragmentationStrategySubject } from './FragmentationStrategySubject';
import { readFileSync } from 'fs';
import { readFile } from 'fs/promises';
import { join } from 'path';
import type { IQuadSink } from '../io/IQuadSink';
import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import * as ShexParser from '@shexjs/parser';
import * as N3 from 'n3';

const DF = new DataFactory<RDF.Quad>();

export class FragmentationStrategyShape extends FragmentationStrategyStreamAdapter {
    private readonly relativePath?: string;
    private readonly tripleShapeTreeLocator?: boolean;
    private readonly shapeMap: Map<string, string>;
    private resourceHandled: Set<string> = new Set();
    private readonly shapeTreeFileName: string = "shapetree.nq";

    private rdfTypeNode = DF.namedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    private shapeTreeNode = DF.namedNode("http://www.w3.org/ns/shapetrees#ShapeTree");
    private shapeTreeShapeNode = DF.namedNode("http://www.w3.org/ns/shapetrees#shape");
    private solidInstance = DF.namedNode("http://www.w3.org/ns/solid/terms#instance");
    private solidInstanceContainer = DF.namedNode("http://www.w3.org/ns/solid/terms#instanceContainer");

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
                const positionResourceIndex = iri.indexOf(`/${resourceIndex}/`);
                if (positionResourceIndex !== -1) {
                    const podIRI = iri.substring(0, positionResourceIndex - 1);
                    const shapeTreeIRI = `${podIRI}/${this.shapeTreeFileName}`;
                    const shapeIRI = `${podIRI}/${resourceIndex}.nq`;
                    const contentIRI = `${podIRI}/${resourceIndex}`;

                    if (this.tripleShapeTreeLocator) {
                        this.generateShapeTreeLocator(quadSink, podIRI, shapeTreeIRI, iri);
                    }
                    this.generateShape(quadSink, shapeIRI, shapePath);
                    this.generateShapetreeTriples(quadSink, shapeTreeIRI, shapeIRI, true, contentIRI);
                    this.resourceHandled.add(iri);
                    return;
                }
            }
        }
    }

    private generateShapeTreeLocator(quadSink: IQuadSink, podIRI: string, shapeTreeIRI: string, iri: string) {
        const shapeTreeIndicator = DF.quad(
            DF.namedNode(podIRI),
            DF.namedNode('http://www.w3.org/ns/shapetrees#ShapeTreeLocator'),
            DF.namedNode(shapeTreeIRI)
        );
        quadSink.push(iri, shapeTreeIndicator);
    }

    private generateShapetreeTriples(quadSink: IQuadSink, shapeTreeIRI: string, shapeIRI: string, isNotInRootOfPod: boolean, contentIri: string) {
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

    private async generateShape(quadSink: IQuadSink, shapeIRI: string, shapePath: string) {
        const shexParser = ShexParser.construct(shapeIRI);
        const shapeShexc = (await readFile(shapePath)).toString();
        const shapeJSONLD = shexParser.parse(shapeShexc);
        const n3Parser = new N3.Parser();
        const shapeQuads = n3Parser.parse(
            JSON.stringify(shapeJSONLD)
        );
        for (const quad of shapeQuads) {
            quadSink.push(shapeIRI, quad)
        }

    }

    protected async flush(quadSink: IQuadSink): Promise<void> {
        await super.flush(quadSink);
    }
}
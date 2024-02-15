import { Readable } from 'stream';
import * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import { FragmentationStrategyShape } from '../../../lib/strategy/FragmentationStrategyShape';
import { readFileSync } from 'fs';
import { readFile } from 'fs/promises';


const streamifyArray = require('streamify-array');
const DF = new DataFactory();

jest.mock('fs');
jest.mock('fs/promises');

describe('FragmentationStrategySubject', () => {
    let strategy: FragmentationStrategyShape;
    let sink: any;



    describe('generateShape', () => {
        beforeEach(() => {
            sink = {
                push: jest.fn(),
            };
        });

        it('should push a single shape inside the sink given a shex shape path with one shape', async () => {
            (<jest.Mock>readFile).mockReturnValueOnce(
                new Promise((resolve) => {
                    resolve({
                        toString: () => {
                            return `
                                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                                PREFIX ldbcvoc: <http://localhost:3000/www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>
                                PREFIX schema: <http://www.w3.org/2000/01/rdf-schema#>

                                <#Post> {
                                    ldbcvoc:id xsd:long {1} ;
                                    ldbcvoc:imageFile xsd:string * ;
                                    ldbcvoc:locationIP xsd:string {1} ;
                                    ldbcvoc:browserUsed xsd:string {1} ;
                                    ldbcvoc:creationDate xsd:dateTime {1} ;
                                    ldbcvoc:hasCreator IRI {1} ;
                                    schema:seeAlso IRI * ;
                                    ldbcvoc:isLocatedIn IRI ? ;
                                }
                            `;
                        }
                    })
                })
            );

            await FragmentationStrategyShape.generateShape(sink, 'http://foo.ca/', 'bar');
            expect(sink.push).toHaveBeenCalledTimes(10);
        });
    });

    describe('generateShapetreeTriples', () => {
        const shapeTreeIRI = "foo";
        const shapeIRI = "bar";
        const contentIri = "boo";

        const orderedInfoInQuad = [FragmentationStrategyShape.shapeTreeNode.value, shapeIRI, contentIri];

        beforeEach(() => {
            sink = {
                push: jest.fn(),
            };
        });

        it('should add into the sink the quad related to the type, the shape and the target and interprete correctly that the data is inside a container in the pod', () => {
            const isNotInRootOfPod = false;
            FragmentationStrategyShape.generateShapetreeTriples(sink, shapeTreeIRI, shapeIRI, isNotInRootOfPod, contentIri);

            expect(sink.push).toHaveBeenCalledTimes(3);
            const calls: any[] = sink.push.mock.calls;
            for (const i in calls) {
                expect(calls[i][0]).toBe(shapeTreeIRI);
                expect((<RDF.Quad>calls[i][1]).object.value).toBe(orderedInfoInQuad[i]);
            }

            expect((<RDF.Quad>calls[2][1]).predicate).toStrictEqual(FragmentationStrategyShape.solidInstanceContainer);
        });

        it('should add into the sink the quad related to the type, the shape and the target and interprete correctly that the data is at the root of the pod ', () => {
            const isNotInRootOfPod = true;
            FragmentationStrategyShape.generateShapetreeTriples(sink, shapeTreeIRI, shapeIRI, isNotInRootOfPod, contentIri);

            expect(sink.push).toHaveBeenCalledTimes(3);
            const calls: any[] = sink.push.mock.calls;
            for (const i in calls) {
                expect(calls[i][0]).toBe(shapeTreeIRI);
                expect((<RDF.Quad>calls[i][1]).object.value).toBe(orderedInfoInQuad[i]);
            }

            expect((<RDF.Quad>calls[2][1]).predicate).toStrictEqual(FragmentationStrategyShape.solidInstance);
        });
    });

    describe('generateShapeTreeLocator', () => {
        const podIRI = "foo";
        const shapeTreeIRI = "bar";
        const iri = "boo";

        beforeEach(() => {
            sink = {
                push: jest.fn(),
            };
        });

        it('should add a shape tree descriptor to the content iri', () => {
            FragmentationStrategyShape.generateShapeTreeLocator(sink, podIRI, shapeTreeIRI, iri);
            expect(sink.push).toHaveBeenCalledTimes(1);
            const call = sink.push.mock.calls;
            expect(call[0][0]).toBe(iri);
            const expectedQuad = DF.quad(
                DF.namedNode(podIRI),
                FragmentationStrategyShape.shapeTreeLocator,
                DF.namedNode(shapeTreeIRI)
            );
            expect((<RDF.Quad>call[0][1])).toStrictEqual(expectedQuad);
        });
    });

});
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
    let sink: any;

    describe('generateShape', () => {
        beforeEach(() => {
            sink = {
                push: jest.fn(),
            };
        });

        it('should push the shape inside the sink given a shex shape path with one shape', async () => {
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
            expect(sink.push).toHaveBeenCalledTimes(81);
        });

        it('should throw given that the shape is not valid', async () => {
            (<jest.Mock>readFile).mockReturnValueOnce(
                new Promise((resolve) => {
                    resolve({
                        toString: () => {
                            return `
                                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                                PREFIX ldbcvoc: <http://localhost:3000/www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>
                                PREFIX schema: <http://www.w3.org/2000/01/rdf-schema#>

                                <#Post> {
                                    SUPER VALID~~~!
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

            await expect(FragmentationStrategyShape.generateShape(sink, 'http://foo.ca/', 'bar')).rejects.toBeDefined();
            expect(sink.push).toHaveBeenCalledTimes(0);
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

    describe('generateShapeIndexInformation', () => {
        let resourceHandled: Set<string> = new Set();
        const iri = "http://localhost:3000/pods/00000000000000000065/posts/2012-05-08#893353212198";
        const positionContainer = 53;
        const resourceIndex = "posts";
        const shapePath = "bar";

        let spyGenerateShapetreeTriples: any;
        let spyGenerateShape:any;
        let spyGenerateShapeTreeLocator:any;

        beforeEach(() => {
            resourceHandled = new Set();
            sink = {
                push: jest.fn(),
            };
            spyGenerateShapetreeTriples = jest.spyOn(FragmentationStrategyShape, 'generateShapetreeTriples');
            spyGenerateShape = jest.spyOn(FragmentationStrategyShape, 'generateShape');
            spyGenerateShapeTreeLocator = jest.spyOn(FragmentationStrategyShape, 'generateShapeTreeLocator');
        });

        afterAll(() => {
            (<jest.Mock>FragmentationStrategyShape.generateShapetreeTriples).mockRestore();
            (<jest.Mock>FragmentationStrategyShape.generateShape).mockRestore();
            (<jest.Mock>FragmentationStrategyShape.generateShapeTreeLocator).mockRestore();
        });

        // https://stackoverflow.com/questions/50421732/mocking-up-static-methods-in-jest
        it("should call the generateShape and the generateShapetreeTriples when the tripleShapeTreeLocator flag is false. It should also add the iri into the resouceHandle set when the tripleShapeTreeLocator flag is false", async () => {
            await FragmentationStrategyShape.generateShapeIndexInformation(sink,
                resourceHandled,
                iri,
                positionContainer,
                resourceIndex,
                shapePath,
                false
            );
            expect(spyGenerateShapeTreeLocator).toHaveBeenCalledTimes(0);
            expect(spyGenerateShape).toHaveBeenCalledTimes(1);
            expect(spyGenerateShapetreeTriples).toHaveBeenCalledTimes(1);
            expect(resourceHandled.size).toBe(1);
            expect(resourceHandled.has(iri)).toBe(true);
        });

        it("should call the generateShape, the generateShapetreeTriples and the generateShapeTreeLocator when the tripleShapeTreeLocator flag is true. It should also add the iri into the resouceHandle set", async () => {
            await FragmentationStrategyShape.generateShapeIndexInformation(sink,
                resourceHandled,
                iri,
                positionContainer,
                resourceIndex,
                shapePath,
                true
            );
            expect(FragmentationStrategyShape.generateShapeTreeLocator).toHaveBeenCalledTimes(1);
            expect(FragmentationStrategyShape.generateShape).toHaveBeenCalledTimes(1);
            expect(FragmentationStrategyShape.generateShapetreeTriples).toHaveBeenCalledTimes(1);
            expect(resourceHandled.size).toBe(1);
            expect(resourceHandled.has(iri)).toBe(true);
        });
    });

    describe('fragment', () => {
        let strategy: FragmentationStrategyShape;
        const shapeFolder = "foo";
        const relativePath = undefined;
        const tripleShapeTreeLocator = true;

        beforeEach(() => {
            sink = {
                push: jest.fn(),
            };

            (<jest.Mock>readFileSync).mockReturnValue(
                {
                    toString: () => `{
                        "shapes": {
                            "comments": "comments.shexc",
                            "posts": "posts.shexc"
                        }
                    }`
                }
            );

            (<jest.Mock>readFile).mockReturnValue(
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

            strategy = new FragmentationStrategyShape(shapeFolder, relativePath, tripleShapeTreeLocator);
        });

        it('should handle an empty stream', async () => {
            await strategy.fragment(streamifyArray([...[]]), sink);
            expect(sink.push).not.toHaveBeenCalled();
        });

        it('should handle a quad that is not bounded by a shape', async () => {
            const quads = [
                DF.quad(
                    DF.blankNode(),
                    DF.namedNode("foo"),
                    DF.namedNode("bar")
                )
            ];
            await strategy.fragment(streamifyArray([...quads]), sink);
            expect(sink.push).not.toHaveBeenCalled();
        });

        it('should handle a quad that is bounded by a shape', async () => {
            const quads = [
                DF.quad(
                    DF.namedNode("http://localhost:3000/pods/00000000000000000267/posts/2011-10-13#687194891562"),
                    DF.namedNode("foo"),
                    DF.namedNode("bar")
                )
            ];
            await strategy.fragment(streamifyArray([...quads]), sink);
            expect(sink.push).toHaveBeenCalledTimes(81 + 3 + 1);
        });
    });

});
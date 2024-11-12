import type * as RDF from '@rdfjs/types';
import * as ShexParser from '@shexjs/parser';
import { JsonLdParser } from 'jsonld-streaming-parser';
import prand from 'pure-rand';
import { DF, DatasetSummary, type IDatasetSummaryOutput, type IDatasetSummaryArgs } from './DatasetSummary';

/* eslint-disable-next-line import/extensions */
import * as SHEX_CONTEXT from './shex-context.json';

const openShape = `
<$> {
  a IRI? ;
}
`
/* eslint-disable ts/naming-convention */
export enum ResourceFragmentation {
  /**
   * The data is in multiple files
   */
  DISTRIBUTED,
  /**
   * The data is in one file
   */
  SINGLE,
}
/* eslint-enable ts/naming-convention */
/**
 * Data that the fragmentation is not described using triples
 * eg: profile, noise and setting in Solidbench
 */
export interface IUndescribedDataModel {
  fragmentation: ResourceFragmentation;
  name: string;
}
/**
 * The entry of a shape index
 */
export interface IShapeIndexEntry {
  /**
   * @description the ShExC string
   */
  shape: string;
  shapeInfo: {
    name: string;
    directory: string;
    dependencies: string[];
  };
  iri: string;
  ressourceFragmentation: ResourceFragmentation;
}

export interface IShapeEntry {
  /**
   * @description the ShExC string
   */
  shapes: string[];
  /**
   * @description the directory targeted by the shape
   */
  directory: string;
  /**
   * @description name of the targeted shape in the schema
   */
  name: string;
  /**
   * @description key in a shape map of the dependent shapes
   */
  dependencies?: string[];
}

export interface IDatasetSummaryShapeIndex extends IDatasetSummaryArgs {
  /**
   * Iri (place at the object position) indicating the fragmentation of the ressource into a single file.
   * For exemple http://localhost:3000/internal/FragmentationOneFile
   */
  iriFragmentationOneFile: Set<string>;
  /**
   * Iri (place at the object position) indicating the fragmentation of the ressource into a multiple files.
   * For exemple http://localhost:3000/internal/FragmentationPerResource
   */
  iriFragmentationMultipleFiles: Set<string>;
  /**
   * Iri (place at the object predicate) indicating the resource being fragmented.
   * For exemple http://localhost:3000/internal/postsFragmentation .
   * The key is the predicate and the object is the name from shape in an IShapeEntry object
   */
  datasetResourceFragmentationPredicate: Record<string, string>;
  /**
   * @description A map of the resource type and shape
   */
  shapeMap: Record<string, IShapeEntry>;
  /**
   * All the content type of a dataset.
   * It is used to determine if the shape index is complete.
   */
  contentTypesOfDatasets: Set<string>;
  /**
   * The random seed for stochastic shape index generation operations.
   */
  randomSeed: number;
  /**
   * Dataset object divided by resource but not described by the fragmentation.
   * The key is an element of the path of those object and the object is the name from shape in an IShapeEntry object.
   * Example: key=>card; object=>card; triple=>http://localhost:3000/pods/00000030786325577964/profile/card#me
   */
  datasetResourceFragmentationException: Record<string, IUndescribedDataModel>;
  /**
   * Probability to generate a shape index.
   * Should be between 0 and 100.
   */
  generationProbability?: number;

  probabilisticGenerationOnEntries?: boolean;
}

export class DatasetSummaryShapeIndex extends DatasetSummary {
  /* eslint-disable ts/naming-convention */
  public static readonly SHAPE_INDEX_FILE_NAME: string = 'shapeIndex';

  public static readonly SHAPE_INDEX_PREFIX = 'https://shapeIndex.com#';

  public static readonly RDF_TYPE_NODE = DF.namedNode('http://www.w3.org/1999/02/22-rdf-syntax-ns#type');
  public static readonly RDF_TRUE = DF.literal('true', DF.namedNode('http://www.w3.org/2001/XMLSchema#boolean'));
  public static readonly RDF_STRING_TYPE = DF.namedNode('http://www.w3.org/2001/XMLSchema#string');

  public static readonly SHAPE_INDEX_LOCATION_NODE = DF.namedNode(`${DatasetSummaryShapeIndex.SHAPE_INDEX_PREFIX}shapeIndexLocation`);
  public static readonly SHAPE_INDEX_CLASS_NODE = DF.namedNode(`${DatasetSummaryShapeIndex.SHAPE_INDEX_PREFIX}ShapeIndex`);
  public static readonly SHAPE_INDEX_ENTRY_NODE = DF.namedNode(`${DatasetSummaryShapeIndex.SHAPE_INDEX_PREFIX}entry`);
  public static readonly SHAPE_INDEX_BIND_BY_SHAPE_NODE = DF.namedNode(`${DatasetSummaryShapeIndex.SHAPE_INDEX_PREFIX}bindByShape`);
  public static readonly SHAPE_INDEX_DOMAIN_NODE = DF.namedNode(`${DatasetSummaryShapeIndex.SHAPE_INDEX_PREFIX}domain`);
  public static readonly SHAPE_INDEX_IS_COMPLETE_NODE = DF.namedNode(`${DatasetSummaryShapeIndex.SHAPE_INDEX_PREFIX}isComplete`);

  public static readonly SOLID_INSTANCE_NODE = DF.namedNode('http://www.w3.org/ns/solid/terms#instance');
  public static readonly SOLID_INSTANCE_CONTAINER_NODE =
    DF.namedNode('http://www.w3.org/ns/solid/terms#instanceContainer');
  /* eslint-enable ts/naming-convention */

  private readonly iriFragmentationOneFile: Set<string>;
  private readonly iriFragmentationMultipleFiles: Set<string>;
  private readonly datasetResourceFragmentationPredicate: Record<string, string>;
  private readonly shapeMap: Record<string, IShapeEntry>;
  private randomGenerator: prand.RandomGenerator;
  /**
   * The registered entries of the  shape index
   */
  private readonly registeredEntries: Map<string, IShapeIndexEntry> = new Map();
  private readonly resourceTypesOfDatasets: Set<string>;
  private readonly datasetResourceFragmentationException: Record<string, IUndescribedDataModel>;

  /**
   * The handled undescribed resources.
   * It exist to avoid repetition of the shape index entries.
   */
  private readonly handledUndescribedResources: Set<string> = new Set();
  /**
   * The IRI of the shape index of the dataset
   */
  private readonly shapeIndexIri: string;
  /**
   * The probability to generate a shape index entry
   */
  private readonly generationProbability: number;

  private readonly doNotRegister: boolean;

  private readonly probabilisticGenerationOnEntries: boolean;

  private hasOpenShapes: boolean = false;

  public constructor(args: IDatasetSummaryShapeIndex) {
    super(args);
    this.iriFragmentationMultipleFiles = args.iriFragmentationMultipleFiles;
    this.iriFragmentationOneFile = args.iriFragmentationOneFile;
    this.datasetResourceFragmentationPredicate = args.datasetResourceFragmentationPredicate;
    this.shapeMap = args.shapeMap;
    this.resourceTypesOfDatasets = args.contentTypesOfDatasets;
    this.datasetResourceFragmentationException = args.datasetResourceFragmentationException;
    this.randomGenerator = prand.xoroshiro128plus(args.randomSeed);
    this.shapeIndexIri = `${this.dataset}/${DatasetSummaryShapeIndex.SHAPE_INDEX_FILE_NAME}`;
    this.generationProbability = args.generationProbability ?? 100;

    this.probabilisticGenerationOnEntries = args.probabilisticGenerationOnEntries ?? false;
    if (!this.probabilisticGenerationOnEntries) {
      const [entryGenerationValue, newGenerator] =
        prand.uniformIntDistribution(0, 100, this.randomGenerator);
      this.randomGenerator = newGenerator;
      // Add the entry to the shape index based on the generation probability
      if (entryGenerationValue < this.generationProbability) {
        this.doNotRegister = false;
      } else {
        this.doNotRegister = true;
      }
    } else {
      this.doNotRegister = false;
    }
  }

  public register(quad: RDF.Quad): void {
    if (this.doNotRegister) {
      return;
    }
    // Register an entry that is described by the data model
    const dataModelObject = this.datasetResourceFragmentationPredicate[quad.predicate.value];
    if (dataModelObject && quad.subject.value.includes(this.dataset)) {
      const fragmentation = this.iriFragmentationMultipleFiles.has(quad.object.value) ?
        ResourceFragmentation.DISTRIBUTED :
        ResourceFragmentation.SINGLE;
      this.registerShapeIndexEntry(dataModelObject, fragmentation);
    }

    // Register an entry undescribed by the data model
    for (const [pathElement, { name, fragmentation }] of Object.entries(this.datasetResourceFragmentationException)) {
      if (quad.subject.value.includes(pathElement) &&
        quad.subject.value.includes(this.dataset) &&
        !this.handledUndescribedResources.has(name)) {
        this.registerShapeIndexEntry(name, fragmentation);
        this.handledUndescribedResources.add(name);
        return;
      }
    }
  }

  /**
   * Register a shape index entry
   * @param {string} dataModelResource
   * @param {ResourceFragmentation} fragmentation
   */
  public registerShapeIndexEntry(dataModelResource: string, fragmentation: ResourceFragmentation): void {
    const shapeEntry = this.shapeMap[dataModelResource];
    if (shapeEntry) {
      const [randomIndex, newGenerator] =
        prand.uniformIntDistribution(0, shapeEntry.shapes.length - 1, this.randomGenerator);
      this.randomGenerator = newGenerator;
      // Choose the a shape from the shape been using with an even probability
      const shape = shapeEntry.shapes[randomIndex];
      const iri = fragmentation === ResourceFragmentation.DISTRIBUTED ?
        `${this.dataset}/${shapeEntry.directory}/` :
        `${this.dataset}/${dataModelResource}`;

      const indexEntry: IShapeIndexEntry = {
        shape,
        shapeInfo: {
          name: shapeEntry.name,
          directory: shapeEntry.directory,
          dependencies: shapeEntry.dependencies ?? [],
        },
        ressourceFragmentation: fragmentation,
        iri,
      };
      this.registeredEntries.set(dataModelResource, indexEntry);
    }
  }

  public async serialize(): Promise<IDatasetSummaryOutput[]> {
    const [shapeIndexEntry, shapes] = await this.serializeShapeIndexEntries();
    if (shapeIndexEntry.quads.length === 0) {
      return [];
    }
    const shapeIndex = this.serializeShapeIndexInstance();
    const shapeIndexCompleteness = this.serializeCompletenessOfShapeIndex();
    shapeIndex.quads = [...shapeIndex.quads, ...shapeIndexEntry.quads, ...shapeIndexCompleteness.quads];
    return [
      shapeIndex,
      ...shapes,
    ];
  }

  /**
   * Serialize the dependencies of a shape recursively
   * @param {string[]} dependencies - Dependencies of a shape
   * @returns {[IDatasetSummaryOutput, IDatasetSummaryOutput[]]} -
   * The serialized shapes
   */
  public async serializeShapeDependencies(dependencies: string[]): Promise<IDatasetSummaryOutput[]> {
    const operationGenerationShapes: Promise<IDatasetSummaryOutput>[] = [];
    const recursiveOperations: Promise<IDatasetSummaryOutput[]>[] = [];

    for (const dependency of dependencies) {
      const shapeEntry = this.shapeMap[dependency];
      const [randomIndex, newGenerator] =
        prand.uniformIntDistribution(0, shapeEntry.shapes.length - 1, this.randomGenerator);
      this.randomGenerator = newGenerator;
      // Choose the a shape from the shape been using with an even probability
      const shape = shapeEntry.shapes[randomIndex];
      operationGenerationShapes.push(
        this.serializeShape(shape, this.generateShapeIri({ directory: shapeEntry.directory, name: shapeEntry.name })),
      );
      recursiveOperations.push(this.serializeShapeDependencies(shapeEntry.dependencies ?? []));
    }

    const generatedShapeFromRecusion = await Promise.all(recursiveOperations);
    const generatedShapes = await Promise.all(operationGenerationShapes);
    let shapeOutputs: IDatasetSummaryOutput[] = generatedShapes;

    for (const summaryOutput of generatedShapeFromRecusion) {
      shapeOutputs = [...shapeOutputs, ...summaryOutput];
    }
    return shapeOutputs;
  }

  /**
   * Serialized the shape index entries and their associated shapes
   * @returns {[IDatasetSummaryOutput, IDatasetSummaryOutput[]]} -
   * The serialized shape index entries and their associated shapes
   */
  public async serializeShapeIndexEntries(): Promise<[IDatasetSummaryOutput, IDatasetSummaryOutput[]]> {
    const output: IDatasetSummaryOutput = {
      iri: this.shapeIndexIri,
      quads: [],
    };
    let shapeOutputs: IDatasetSummaryOutput[] = [];

    const shapeIndexNode = DF.namedNode(this.shapeIndexIri);
    for (const [key, entry] of this.registeredEntries) {
      const currentEntry = DF.blankNode(entry.shapeInfo.name);
      const entryTypeDefinition = DF.quad(
        shapeIndexNode,
        DatasetSummaryShapeIndex.SHAPE_INDEX_ENTRY_NODE,
        currentEntry,
      );
      const bindByShape = DF.quad(
        currentEntry,
        DatasetSummaryShapeIndex.SHAPE_INDEX_BIND_BY_SHAPE_NODE,
        DF.namedNode(this.generateShapeIri(entry.shapeInfo)),
      );
      const target = DF.quad(
        currentEntry,
        entry.ressourceFragmentation === ResourceFragmentation.SINGLE ?
          DatasetSummaryShapeIndex.SOLID_INSTANCE_NODE :
          DatasetSummaryShapeIndex.SOLID_INSTANCE_CONTAINER_NODE,
        DF.namedNode(entry.iri),
      );
      let shape = entry.shape;
      if (this.probabilisticGenerationOnEntries) {
        const [entryGenerationValue, newGenerator] =
          prand.uniformIntDistribution(0, 100, this.randomGenerator);
        this.randomGenerator = newGenerator;
        // given a probability give use an open shape
        if (entryGenerationValue >= this.generationProbability) {
          shape = openShape;
          this.hasOpenShapes = true;
        }
      }
      shapeOutputs.push(await this.serializeShape(shape, this.generateShapeIri(entry.shapeInfo)));
      shapeOutputs = [...shapeOutputs, ...(await this.serializeShapeDependencies(entry.shapeInfo.dependencies))];
      output.quads.push(entryTypeDefinition, bindByShape, target);

    }
    return [output, shapeOutputs];
  }

  /**
   * Serialize the triples that define the shape index instance
   */
  public serializeShapeIndexInstance(): IDatasetSummaryOutput {
    const shapeIndexNode = DF.namedNode(this.shapeIndexIri);
    const typeDefinition = DF.quad(
      shapeIndexNode,
      DatasetSummaryShapeIndex.RDF_TYPE_NODE,
      DatasetSummaryShapeIndex.SHAPE_INDEX_CLASS_NODE,
    );

    const domain = DF.quad(
      shapeIndexNode,
      DatasetSummaryShapeIndex.SHAPE_INDEX_DOMAIN_NODE,
      DF.literal(`${this.dataset}/.*`, DatasetSummaryShapeIndex.RDF_STRING_TYPE),
    );

    return {
      iri: this.shapeIndexIri,
      quads: [typeDefinition, domain],
    };
  }

  /**
   * Serialize the information indicating if the shape index is complete
   */
  public serializeCompletenessOfShapeIndex(): IDatasetSummaryOutput {
    if (!this.hasOpenShapes) {
      // All the resource has been handled so we indicate it with a triple
      const isComplete = DF.quad(
        DF.namedNode(this.shapeIndexIri),
        DatasetSummaryShapeIndex.SHAPE_INDEX_IS_COMPLETE_NODE,
        DatasetSummaryShapeIndex.RDF_TRUE,
      );

      return {
        iri: this.shapeIndexIri,
        quads: [isComplete],
      };
    }
    return {
      iri: this.shapeIndexIri,
      quads: [],
    }
  }

  /**
   * Serialize the shape
   * @param {string} shapeShexc ShEx template shapes in the shexc serialized
   * @param {string} shapeIRI The Iri of the shape
   */
  public async serializeShape(shapeShexc: string, shapeIRI: string): Promise<IDatasetSummaryOutput> {
    const shexParser = ShexParser.construct(shapeIRI);
    shapeShexc = this.transformShapeTemplateIntoShape(shapeShexc, shapeIRI);
    const shapeJSONLD = shexParser.parse(shapeShexc);
    const stringShapeJsonLD = JSON.stringify(shapeJSONLD);
    const quads: RDF.Quad[] = [];

    return new Promise((resolve, reject) => {
      // The jsonLD is not valid without the context field and the library doesn't include it
      // because a ShExJ MAY contain a @context field
      // https://shex.io/shex-semantics/#shexj
      const jsonldParser = new JsonLdParser({
        streamingProfile: false,
        context: SHEX_CONTEXT,
        skipContextValidation: true,
      });
      jsonldParser
        .on('data', (quad: RDF.Quad) => {
          quads.push(quad);
        })
        // We ignore this because it is difficult to provide a valid ShEx document that
        // would not be parsable in RDF given it has been already parsed in ShExJ

        .on('error', /* istanbul ignore next */(error: any) => {
          reject(error);
        })
        .on('end', () => {
          resolve({
            iri: shapeIRI,
            quads,
          });
        });

      jsonldParser.write(stringShapeJsonLD);
      jsonldParser.end();
    });
  }

  /**
   * Generate the iri of a shape based on the user provided information
   * @param {{ directory: string; name: string }} entry
   */
  public generateShapeIri(entry: { directory: string; name: string }): string {
    return `${this.dataset}/${entry.directory}_shape#${entry.name}`;
  }

  /**
   * Transform the shape template into a shape
   * @param {string} shapeShexc ShEx template shapes in the shexc serialized
   * @param {string} shapeIRI The Iri of the shape
   */
  public transformShapeTemplateIntoShape(shapeShexc: string, shapeIRI: string): string {
    shapeShexc = shapeShexc.replace('$', shapeIRI);
    for (const entry of Object.values(this.shapeMap)) {
      shapeShexc = shapeShexc.replaceAll(`{:${entry.name}}`, this.generateShapeIri(entry));
    }
    return shapeShexc;
  }
}

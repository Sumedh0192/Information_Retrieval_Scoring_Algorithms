/**********************************************************************
 * Class Name : TAAT_DAAT 
 * Details : This class is used to create an inverted index using Apache Lucene Library and use it to run 
 * 			test cases for TAAT and DAAT to retrieve appropriate document postings depending on the type of query.
 * 			No collections/interfaces have been used to store the inverted index apart from HashMap.
 * 			Terms are accessed sequentially and not order of their size of postings list.
 * Created By : Sumedh Ambokar
*/

import java.io.*;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.HashMap;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.FSDirectory;


public class TAAT_DAAT {
	
	/*	
	 * Method Name: Main
	 * Description:	Executable method. Takes path of the Indexed files, Input file and output file as parameters. 
	 * Parameters: String[] args
	 * Return Type: void
	 */
	public static void main(String[] args) throws IOException, IOException {
		HashMap<String, DocList> DictionaryPostingsMap = new HashMap<String, DocList>();
		// TODO Auto-generated method stub
		//String path = args[0];	// Set the path passed as the argument to the executable main function
		String path = "B:/UB/Semester 1/Information Retrieval/Project 2/index";
		FileSystem fs = FileSystems.getDefault();
		Path path1 = fs.getPath(path);
		try {
			IndexReader reader = DirectoryReader.open(FSDirectory.open(path1));		// Create a reader to read from the indexed files stored at the given path
			Fields fields = MultiFields.getFields(reader);		// Get all the fields from the indexed files
	        for (String field : fields) {		// Iterate on the fields
	        	if(!field.equals("_version_") && !field.equals("id")){		// Excluding the Id and _version_ fields while creating inverted index
		            TermsEnum termsEnum = MultiFields.getTerms(reader, field).iterator();		// Fetch all the terms from each field
		            while (termsEnum.next() != null) {													// Iterate on each term and create a HashMap with the key as the 
		            	if(DictionaryPostingsMap.get(termsEnum.term().utf8ToString()) == null){			// term and the document list stored in Class Object DocList as the value
		            		DictionaryPostingsMap.put(termsEnum.term().utf8ToString(), new DocList());	
			            }
		            	PostingsEnum posting = MultiFields.getTermDocsEnum(reader, field, termsEnum.term());
		            	while (posting.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
		            		if(termsEnum.term().utf8ToString().equals("amtssparekas")){
			            	}
	            			DictionaryPostingsMap.get(termsEnum.term().utf8ToString()).addDoc(posting.docID());
	            		}
		            }
	        	}
	        }

	        BufferedReader buffrdr = new BufferedReader(new InputStreamReader
					(new FileInputStream(args[2]), "UTF8"));		// Creating a buffered reader to read from the input file pass as an argument to executable main function
			Writer Filewtr = new BufferedWriter(new OutputStreamWriter
					(new FileOutputStream(args[1]), "UTF8"));		// Creating a Writer to write in the output file pass as an argument to executable main function
			
	        String line = "";
			while ((line = buffrdr.readLine()) != null){	
				printOutput(line.split(" "),DictionaryPostingsMap,Filewtr);		// Print the results for TAAT/DAAT in the output file for each of the term queries
			}
			buffrdr.close();
			Filewtr.close();
			reader.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	/*	
	 * Method Name: printOutput 
	 * Description: This method is used to calculate TAAT/DAAT result based on the query passed and print the result in the output file.
	 * Parameters: String[] Terms, HashMap<String, DocList> DictionaryPostingsMap, Writer Filewtr
	 * Return Type: void
	 */
	public static void printOutput(String[] Terms, HashMap<String, DocList> DictionaryPostingsMap, Writer Filewtr) throws IOException, IOException{
		String[] Operations = {"And","Or"}; 
		String[] Types = {"Taat","Daat"};
		ComparisonsDocList matchedPostingsList = null;
		for(String term : Terms){		// Write into the output file the postings list of each term
			Filewtr.write("GetPostings\n" + term + "\nPostings list:" + printDocList(term, DictionaryPostingsMap) + "\n");
		}
		for(String type : Types){
			for(String operation : Operations){
				if(type == "Taat"){
					matchedPostingsList = getPostingsMatchTAAT(Terms,operation,DictionaryPostingsMap);		// Call the TAAT function and pass the operation requested as the parameter
				}else if(type == "Daat"){
					if(operation == "And"){
						matchedPostingsList = getPostingsMatchDAATAND(Terms,DictionaryPostingsMap);		// Call the DAAT function to calculate DAAT AND						
					}else{
						matchedPostingsList = getPostingsMatchDAATOR(Terms,DictionaryPostingsMap);		// Call the DAAT function to calculate DAAT OR
					}
				}
				Filewtr.write(type + operation + "\n");
				for(String term : Terms){
					Filewtr.write(term + " ");
				}
				// Write all the results in the provided format to the output file
				
				Filewtr.write("\nResults:" + matchedPostingsList.docList.printDocList() + "\n");
				Filewtr.write("Number of documents in results: " + matchedPostingsList.docList.getDocListLength() + "\n");
				Filewtr.write("Number of comparisons: " + matchedPostingsList.numberOfComparisons + "\n");
				
			}
		}
	}
	
	/*	
	 * Method Name: getPostingsMatchTAATScoring
	 * Description: Calculate TAAT AND and OR comparisons using the scoring method. 
	 * Parameters: String[] QueryTerms, String Operation, HashMap<String, DocList> DictionaryPostingsMap
	 * Return Type: ComparisonsDocList
	 */
	
	/* METHOD HAS BEEN COMMENTED AS NOT BEEN USED
	public static ComparisonsDocList getPostingsMatchTAATScoring(String[] QueryTerms, String Operation, HashMap<String, DocList> DictionaryPostingsMap){
		HashMap<Integer, Integer> TermDocCountTAAT = new HashMap<Integer, Integer>(); // This Map stores the number of occurrences of a document from term postings against the document ID
		if(QueryTerms.length > 0){
			DocList matchedDocs = new DocList();
			Doc tempDoc;
			Integer numberOfComparisons = 0;
			for(Integer i = 0; i < QueryTerms.length; i++){
				tempDoc = getDocList(QueryTerms[i], DictionaryPostingsMap).getDocListHead();
				while(tempDoc != null){
					if(TermDocCountTAAT.get(tempDoc.docId) == null){
	            		TermDocCountTAAT.put(tempDoc.docId, 1);
		            }else{
		            	TermDocCountTAAT.put(tempDoc.docId,TermDocCountTAAT.get(tempDoc.docId) + 1);
		            }
					tempDoc = tempDoc.next;
				}
			}
			if(TermDocCountTAAT.size() > 0){
				for(Integer docId : TermDocCountTAAT.keySet()){
					if(QueryTerms.length == TermDocCountTAAT.get(docId)){
						matchedDocs.addDoc(docId);;
					}else if(Operation == "Or"){
						matchedDocs.addDoc(docId);
					}
				}
			}
			return new ComparisonsDocList(matchedDocs,numberOfComparisons);
		}
		return null;
	}
	*/
	
	/*	
	 * Method Name: getPostingsMatchTAAT
	 * Description: This method is used to get the output of applying TAAT AND/OR operations on terms using intermediate list.
	 * 				It uses the concept of finding 
	 * Parameters: String[] QueryTerms, String Operation, HashMap<String, DocList> DictionaryPostingsMap
	 * Return Type: ComparisonsDocList
	 */
	public static ComparisonsDocList getPostingsMatchTAAT(String[] QueryTerms, String Operation, HashMap<String, DocList> DictionaryPostingsMap){
		if(QueryTerms.length > 0){
			if(QueryTerms.length == 1){
				return new ComparisonsDocList(getDocList(QueryTerms[0], DictionaryPostingsMap),0); // If there is only one term in the query
			}else{
				DocList intermediateDocList = new DocList();	// This DocList acts as the intermediate list to store the compared results from the 2 comparing lists
				Doc tempDoc, comparingDoc;		// These 2 Docs are used to iterate on the 2 Doclists for comparison
				Integer numberOfComparisons = 0;
				tempDoc = getDocList(QueryTerms[0], DictionaryPostingsMap).getDocListHead();
				Boolean first = true;
				for(Integer i = 1; i < QueryTerms.length; i++){
					if(!first){		// Do not execuate the below code for the first loop of comparison
						tempDoc = intermediateDocList.getDocListHead(); // Set the tempDoc to the head of the intermediate list after every comparison round
						intermediateDocList = new DocList();	// Re-initiate the intermediate list so that it can be used for the new round of the comparison
					}else{
						first = false;
					}
					comparingDoc = getDocList(QueryTerms[i], DictionaryPostingsMap).getDocListHead();	// Set the comparingDoc to the head of the new Doc Posting list
					while(tempDoc != null && comparingDoc != null){		// Continue to iterate until one of the 
						if(tempDoc.docId == comparingDoc.docId){		// If the 2 document ids are same then move to the next Docs from the lists for both comparing postings list
							intermediateDocList.addDoc(comparingDoc.docId);
							tempDoc = tempDoc.next;
							comparingDoc = comparingDoc.next;
						}else if(tempDoc.docId > comparingDoc.docId){	// If tempDoc's document id is greater then move the pointer of comparingDoc to its next Doc
							if(Operation == "Or"){
								intermediateDocList.addDoc(comparingDoc.docId); // For Or Operation the smaller document id is added to the intermediate list
							}
							comparingDoc = comparingDoc.next;
						}else{		// If comparingDoc's document id is greater then move the pointer of tempDoc to its next Doc
							if(Operation == "Or"){	
								intermediateDocList.addDoc(tempDoc.docId); // For Or Operation the smaller document id is added to the intermediate list
							}
							tempDoc = tempDoc.next;
						}
						numberOfComparisons++; // Increment the comparison count for every round of comparison between tempDoc and comparingDoc
					}
					if(Operation == "Or"){		// For Or Operation when only one postings list still has documents left, then add all the document ids to the intermediate list
						if(comparingDoc != null){
							while(comparingDoc != null){
								intermediateDocList.addDoc(comparingDoc.docId);
								comparingDoc = comparingDoc.next;
							}
						}else if(tempDoc != null){
							while(tempDoc != null){
								intermediateDocList.addDoc(tempDoc.docId);
								tempDoc = tempDoc.next;
							}
						}
					}
				}
				return new ComparisonsDocList(intermediateDocList,numberOfComparisons);		// Return the intermediate list and the comparisons to the calling function using ComparisonsDocList Object
			}
		}
		return null;
	}
	
	/*	
	 * Method Name: getPostingsMatchDAATAND
	 * Description: This method is used to get the output of applying DAAT AND operation on terms using parallel pointers.
	 * Parameters: String[] QueryTerms, HashMap<String, DocList> DictionaryPostingsMap
	 * Return Type: ComparisonsDocList
	 */
	public static ComparisonsDocList getPostingsMatchDAATAND(String[] QueryTerms, HashMap<String, DocList> DictionaryPostingsMap){
		if(QueryTerms.length > 0){
			if(QueryTerms.length == 1){
				return new ComparisonsDocList(getDocList(QueryTerms[0], DictionaryPostingsMap),0);	// If there is only one term in the query
			}else{
				DocList matchedDocs = new DocList();	// Stores the document ids of the documents which match the DAAT AND criteria
				Doc[] docPointers = new Doc[QueryTerms.length];		// Stores the pointers to individual Doclists of the comparing terms
				Integer maxDocId = null;	// used to store the greatest document id amongst the comparing Doclists which are pointed by the respective pointers
				Integer docMatchedCounter = 0;
				Boolean breakLoop = false;
				Integer numberOfComparisons = 0;
				for(Integer i = 0; i < QueryTerms.length; i++){	
					docPointers[i] = getDocList(QueryTerms[i], DictionaryPostingsMap).getDocListHead();		// Set the pointer to the head of every comparing posting list
				}
				while(!breakLoop){
					docMatchedCounter = 0;	// reset the number of comparisons to be used for new comparison round
					for(Integer i = 0; i < docPointers.length && docPointers[i] != null; i++){		// iterate over the list of the pointers
						if(maxDocId == null){		// set the maxDocId to the first document of the first list to initiate the comparisons
							maxDocId = docPointers[0].docId;
						}else{
							if(docPointers[i].docId == maxDocId){		// If the maxDocId is equal than the comparing doc id the increase the pointer value of the comparing list
								docMatchedCounter++;	// increase the number of matches count for that maxDocId
								numberOfComparisons++;
							}else if(docPointers[i].docId > maxDocId){		// If the maxDocId is less to the comparing doc id then set the maxDocID to the comparing document id
								maxDocId = docPointers[i].docId;
								numberOfComparisons++;
							}else{		// If the maxDocId is greater than the comparing doc id then increment the pointers to the comparing list and move along the comparing list 
										//	until the comparing doc id is greater than or equal to the maxDocId
								while(docPointers[i] != null && docPointers[i].docId < maxDocId){
									docPointers[i] = docPointers[i].next;
									numberOfComparisons++;
								}
							}
						}
						// Number of comparisons are considered for every comparison between 2 document ids and is calculated using numberOfComparisons variables
						if(docPointers[i] == null){
							breakLoop = true;
							break;
						}
					}
					if(docMatchedCounter == docPointers.length){	// if the number of matches for a maxDocId is equal to number of term then add it to the output DocList
						matchedDocs.addDoc(maxDocId);
						for(Integer i = 0; i < docPointers.length; i++){	// increment the pointers of all the comparing list
							docPointers[i] = docPointers[i].next;
							if(docPointers[i] == null){
								breakLoop = true;
								break;
							}
						}
					}
					
				}
			return new ComparisonsDocList(matchedDocs,numberOfComparisons);
			}
		}
		return null;
	}
	
	/*	
	 * Method Name: getPostingsMatchDAATOR
	 * Description: This method is used to get the output of applying DAAT OR operation on terms using parallel pointers.
	 * Parameters: String[] QueryTerms, HashMap<String, DocList> DictionaryPostingsMap
	 * Return Type: ComparisonsDocList
	 */
	public static ComparisonsDocList getPostingsMatchDAATOR(String[] QueryTerms, HashMap<String, DocList> DictionaryPostingsMap){
		if(QueryTerms.length > 0){
			if(QueryTerms.length == 1){
				return new ComparisonsDocList(getDocList(QueryTerms[0], DictionaryPostingsMap),0);	// If there is only one term in the query
			}else{
				DocList matchedDocs = new DocList();  // Stores the document ids of the documents which match the DAAT OR criteria
				Doc[] docPointers = new Doc[QueryTerms.length];		// Stores the pointers to individual Doclists of the comparing terms
				Integer minPointerIndex = null;		// used to store the index of the comparing list which has the minimum document ids pointed by the pointers
				Integer numberOfComparisons = 0;
				Boolean allCompareListNotNull;
				Boolean breakLoop = false;
				for(Integer i = 0; i < QueryTerms.length; i++){
					docPointers[i] = getDocList(QueryTerms[i], DictionaryPostingsMap).getDocListHead();		// Set the pointer to the head of every comparing posting list
				}
				while(!breakLoop){
					for(Integer i = 0; i < docPointers.length; i++){		// iterate over the list of the pointers
						if(docPointers[i] != null){
							if(minPointerIndex == null){	
								minPointerIndex = i;	// set the minimum pointer index to the first non null comparing posting list pointer index
							}else if(docPointers[minPointerIndex] != null){
								if(docPointers[minPointerIndex].docId > docPointers[i].docId  && minPointerIndex != i){
								// if the document id pointed by the minPointerIndex is greater than the comparing docId then set the minPointerIndex to the index of the comparing posting list pointer
									minPointerIndex = i;
									numberOfComparisons++;
								}else if(docPointers[minPointerIndex].docId == docPointers[i].docId && minPointerIndex != i){
								// if the document id pointed by the minPointerIndex is equal to the comparing docId then move the pointer of the comparing posting list to its next element
									docPointers[i] = docPointers[i].next;	
									numberOfComparisons++;
								}else if(docPointers[minPointerIndex].docId < docPointers[i].docId && minPointerIndex != i){
									numberOfComparisons++;
								}
								// Number of comparisons are considered for every comparison between 2 document ids and is calculated using numberOfComparisons variable
							}else{
								break;
							}
						}
					}
					if(docPointers[minPointerIndex] == null){	// If a comparing posting list reaches to its end
						allCompareListNotNull = false;
						for(Integer i = 0; i < docPointers.length; i++){	// check if all the comparing posting lists have reached their end
							if(docPointers[i] != null){		// if a comparing posting list still has documents left
								allCompareListNotNull = true;
							}
						}
						if(!allCompareListNotNull){		// if all comparing posting list are null then break
							break;
						}else{
							minPointerIndex = null;
						}
					}else{		// Else add the minimum document id to the output DocList and move the pointer of the list pointed by minPointerIndex to its next document
						matchedDocs.addDoc(docPointers[minPointerIndex].docId);
						docPointers[minPointerIndex] = docPointers[minPointerIndex].next;
					}
				}
				return new ComparisonsDocList(matchedDocs,numberOfComparisons);
			}
		}
		return null;
	}
		
	/*	
	 * Method Name: getDocList
	 * Description: This method is used to get the list of all the document postings for a given term.
	 * Parameters: String term, HashMap<String, DocList> DictionaryPostingsMap
	 * Return Type: String
	 */
	public static DocList getDocList(String term, HashMap<String, DocList> DictionaryPostingsMap){
		if(DictionaryPostingsMap.get(term) != null){
			return DictionaryPostingsMap.get(term);
		}
		return null;
	}
	
	/*	
	 * Method Name: printDocList
	 * Description: This method is used to get the list of all the document postings for a given term in form of Strings
	 * Parameters: String term, HashMap<String, DocList> DictionaryPostingsMap
	 * Return Type: String
	 */
	public static String printDocList(String term, HashMap<String, DocList> DictionaryPostingsMap){
		String str = "";
		if(DictionaryPostingsMap.get(term) != null){
			Doc docIterator = getDocList(term, DictionaryPostingsMap).getDocListHead();
			while(docIterator != null){
				str += " " + docIterator.docId;
				docIterator = docIterator.next;
			}
			return str;
		}
		return " empty";
	}
	
}


/*	
 * Class Name: DocList
 * Description: Used to store the Doc objects in form of a list
 */
class DocList{
	private Doc dochead;
	private Integer docListLength;
	
	/*	
	 * Method Name: DocList
	 * Description:	Constructor of Class Doclist. Assigns the passed Doc object as the head of the Doclist
	 * Parameters: void
	 * Return Type: N/A
	 */
	public DocList(Doc dochead){
		this.dochead = dochead;
		this.docListLength = 1;
	}
	
	/*	
	 * Method Name: DocList
	 * Description:	Constructor of Class Doclist. Initiates the Doclist.
	 * Parameters: void
	 * Return Type: N/A
	 */
	public DocList(){
		this.dochead = null;
		this.docListLength = 0;
	}
	
	/*	
	 * Method Name: getDocListHead
	 * Description: This method is used to fetch the head Doc object of the doclist
	 * Parameters: void 
	 * Return Type: Doc
	 */
	public Doc getDocListHead(){
		return this.dochead;
	}
		
	/*	
	 * Method Name: getDocListLength
	 * Description: This method is used to fetch the length of the doclist
	 * Parameters: void
	 * Return Type: Integer
	 */
	public Integer getDocListLength(){
		return this.docListLength;
	}

	/*	
	 * Method Name: addDoc
	 * Description: This method is used to add new doc to a doclist. The docs are first compared and then inserted, so that the doclist is always sorted.
	 * Parameters: Integer docID
	 * Return Type: void
	 */
	public void addDoc(Integer docID){
		if(this.dochead == null){
			this.dochead = new Doc(docID,null);
		}else{
			Boolean check = false;
			Doc head = dochead;
			Doc tempDoc;
			if(docID < head.docId){
				this.dochead = new Doc(docID, head);
			}else{
				while(head.next != null){
					tempDoc = head;
					head = head.next;
					if(docID <= head.docId){
						tempDoc.next = new Doc(docID, head);
						check = true;
						break;
					}
				}
				if(!check){
					head.next = new Doc(docID, null);
				}
			}
		}
		this.docListLength++;
	}
	
	/*	
	 * Method Name: searchDoc
	 * Description: This method is used to search a particular document from the doclist and return true if the element is present
	 * Parameters: Integer docId
	 * Return Type: boolean
	 */
	public boolean searchDoc(Integer docId){
		if(this.dochead != null){
			Doc tempDoc = this.dochead;
			do{
				if(tempDoc.docId == docId){
					return true;
				}else{
					tempDoc = tempDoc.next;
				}
			}while(tempDoc != null);
		}
		return false;
	}
	
	/*	
	 * Method Name: printDocList
	 * Description: Used to return all the elements of the doclist in form of a string
	 * Parameters: void
	 * Return Type: String
	 */
	public String printDocList(){
		if(this.dochead != null){
			String doclistString = "";
			Doc tempDoc = this.dochead;
			do{
				doclistString += " " + tempDoc.docId;
				tempDoc = tempDoc.next;
			}while(tempDoc != null);
			return doclistString;
		}
		return " empty";
	}
}

/*	
 * Class Name: Doc
 * Description: Stores a document id and a pointer to next document. Used to create doclists.
 */
class Doc{
	public int docId;
	public Doc next;
	
	/*	
	 * Method Name: Doc
	 * Description: Constructor for class Doc. Creates a Doc class object from passed parameters
	 * Parameters: int docId, Doc next
	 * Return Type: N/A
	 */
	public Doc(int docId, Doc next){
		this.docId = docId;
		this.next = next;
	}
}

/*	
 * Class Name: ComparisonsDocList
 * Description: Class created to store number of comparisions after TAAT/DAAT operations along with the result doclist.
 */
class ComparisonsDocList{
	public DocList docList;
	public Integer numberOfComparisons;
	
	/*	
	 * Method Name: ComparisonsDocList
	 * Description: Constructor for class ComparisonsDocList. Creates a ComparisonsDocList class object from passed parameters
	 * Parameters: DocList docList, Integer numberOfComparisons
	 * Return Type: N/A
	 */
	public ComparisonsDocList(DocList docList, Integer numberOfComparisons){
		this.docList = docList;
		this.numberOfComparisons = numberOfComparisons;
	}
}

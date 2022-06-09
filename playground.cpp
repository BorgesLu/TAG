#include <iostream>
#include <iostream>
#include <fstream>
#include <sstream>
#include <typeinfo>
#include <cstring>
#include <array>
#include <chrono>  

#include "include/viper.hpp"

int main(int argc, char **argv)
{
    const size_t initial_size = 1073741824; // 1 GiB
    // auto viper_db = viper::Viper<uint64_t, uint64_t>::create("/mnt/pmem2/viper", initial_size);
    auto viper_db = viper::Viper<uint64_t, uint64_t>::create("/mnt/pmem0/test-lmx", initial_size);

    // To modify records in Viper, you need to use a Viper Client.
    auto v_client = viper_db->get_client();

    // insert vertex
/*    
    std::ifstream inFile("../graph-500/vertex100.csv", ios::in);
    std::string lineStr;
  //  std::array<uint64_t,100> getVertex;
  //  int var = 0 ;
    while (getline(inFile, lineStr))
    {
        uint64_t VID = std::stoul(lineStr);
        const uint64_t VIDKey = VID << 32;
      //  getVertex[var] = VIDKey;
      //  std::cout<<VID<<std::endl;
      //  std::cout<<VIDKey<<std::endl;
      //  var++;
        v_client.put(VIDKey, VID);
    }
    inFile.close();
*/
   uint64_t id = 0;
   while(id<4039){
       uint64_t VIDKey =id << 32;
       v_client.put(VIDKey,id);
       id++;
   }   
   std::cout<<"end of Vertex" <<std::endl;

    // insert edge
    std::ifstream inEdgeFile("./facebook/facebook_combined.txt", ios::in);
    std::string edgelineStr;
  int edgecount = 0;
     while (getline(inEdgeFile, edgelineStr))
	{
		stringstream ss(edgelineStr);
		string str;
		std::array<uint64_t,2> edge;
		int i = 0;
		while (getline(ss, str,' '))
		{
			uint64_t num = stoul(str);
			edge[i] = num;
            //std::cout<<num<<"\n";
			i++;
			//cout << str;		
		}
        edgecount++;
        std::cout<<edgecount<<std::endl;
        uint64_t edgeKey = edge[0];
        edgeKey = edgeKey<<32;
        edgeKey =edgeKey +  edge[1];
        uint64_t desVID = edge[1];
        v_client.put(edgeKey, desVID);
    }
    inEdgeFile.close();

std::cout<<"end of edge \n";

//get Sample SrcVID
    std::ifstream sampleFile("./facebook/facebook-kn-sample.txt", ios::in);
	std::string sampleLineStr;
    std::vector<uint64_t>  sampleVID;
  // uint64_t var = 0;
//获取ID从0到99的邻居顶点    
   // while(var<100){
    //    sampleVID.push_back(var);
    //    var++;
   // }
    while(getline(sampleFile,sampleLineStr)){
        uint64_t VID = std::stoul(sampleLineStr);
        sampleVID.push_back(VID);
    }
    sampleFile.close();

    std::cout<<"sample size"<<sampleVID.size()<<"\n";

double total_Index_time =0;
double total_Pmem_time =0;

//执行三次
 std::ofstream ofs;
for(int i=0;i<3;i++){
//getNeighbors from Pmem
  // getNeighbors from Pmem
    int countFromPmem = 0;
    auto startPmem = std::chrono::steady_clock::now();
    for (uint64_t srcVID : sampleVID)
    {
        int count = 0;
        v_client.get_khop_neighbors_from_data(srcVID, count);
       // if(count>0)
        // std::cout<<count<<std::endl;
        if(count>0){
            countFromPmem += count;
        }
    }
    auto endPmem = std::chrono::steady_clock::now();
    std::chrono::duration<double, std::milli> fp_ms = endPmem - startPmem;

//记录实验的时间
    std::chrono::time_point<std::chrono::system_clock> start = std::chrono::system_clock::now();
    std::time_t tt = std::chrono::system_clock::to_time_t(start);
    std::string stt = std::ctime(&tt);
   // std::ofstream ofs;
    
    std::cout<< "countFromPmem: " << countFromPmem << "\n";

    ofs.open("result.txt", std::ios::app);
    ofs << "第: " << i <<"次实验"<< "\n";
    ofs << "Local time: " << stt << "\n";
    ofs << "Pmem:"<< "\n";

    ofs << "countFromPmem:  " << countFromPmem << "\n";
    ofs << "time: " << fp_ms.count() << "ms"<< "\n";
    
    total_Pmem_time +=  fp_ms.count();

    ofs.close();

    // get neighbors form index
    auto startIndex = std::chrono::steady_clock::now();


    uint64_t countFromIndex = 0;
  //  uint64_t count = 0;
    for (uint64_t VID : sampleVID)
    {
        int hop = 1;     
        int count = 0;
      //  v_client.get_khop_neighbors_from_index(VID, hop, &count);
        v_client.get_khop_neighbors_from_index(VID, hop, count);
       // if(count>0)
        // std::cout<<count<<std::endl;
        countFromIndex += count;
    }
   // std::cout << "countFromIndex  " << countFromIndex << "\n";
    auto endIndex = std::chrono::steady_clock::now();
    std::chrono::duration<double, std::milli> index_ms = endIndex - startIndex;

    //std::cout << "time: " << index_ms.count() << "\n\n";
    //std::cout<<"countFromIndex  "<<countFromIndex<<std::endl;

    
    // ofs.open("result.txt", std::ios::out);
    ofs.open("result.txt", std::ios::app);

    ofs << "Index:"
        << "\n";

    ofs << "countFromIndex:  " << countFromIndex << "\n";
    ofs << "time: " << index_ms.count() << "ms"
        << "\n\n";
    total_Index_time +=  index_ms.count();

    ofs.close();
}


int loop  = 3;
double avg_Pmem_time = total_Pmem_time/loop;
double avg_Index_time = total_Index_time/loop;
//记录平均时间
ofs.open("result.txt", std::ios::app);
ofs<<"Pmem的平均时间"<<avg_Pmem_time<<"\n";
ofs<<"Index的平均时间"<<avg_Index_time<<"\n";




    /*
    for (uint64_t j = 0; j < 100; ++j)
    {
        uint64_t value;
        auto key = getVertex[j];
        const bool found = v_client.get(key, &value);
        if (found)
        {
            std::cout << "Record: " << key << " --> " << value << std::endl;
        }
        else
        {
            std::cout << "No record found for key: " << key << std::endl;
        }
    }
    */

  return 0;
}

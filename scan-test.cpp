#include <iostream>
#include <iostream>
#include <fstream>
#include <sstream>
#include <typeinfo>
#include <cstring>
#include <array>
#include <chrono>

#include "include/viper.hpp"

// schemaID
static constexpr uint32_t PERSON = 0x10000000;
static constexpr uint32_t COMMENT = 0x20000000;
static constexpr uint32_t POST = 0x30000000;

void vertex_writer(viper::Viper<std::string, std::string>::Client &client, std::string filename, uint32_t schemaID)
{
  std::ifstream inFile(filename, std::ios::in);
  std::string lineStr;

  int lineNum{0};
  size_t col{0};
  while (getline(inFile, lineStr))
  {

    std::stringstream ss(lineStr);
    std::string str;
    if (lineNum == 0)
    {                                    // 处理第一行
      while (std::getline(ss, str, '|')) //每一行不同列的分隔符
      {
        col++;
      }
      lineNum++;
        //std::cout<<"col num :\t"<<col<<"\n";
    } // end of if
  

    //插入数据
    else
    {
      std::string Value(col, '0'); ////预留头部 offset的位置
       Value.at(0) = col; //value 的第一个字节保存 map的长度
      int colIndex{0};
      uint64_t key;
      while (std::getline(ss, str, '|')) //每一行不同列的分隔符
      {

        
        if (colIndex == 0)
        { //第一列
          uint32_t VID = std::stoi(str);
          VID |= schemaID;
          key = VID;
          key = key << 32;
         // std::cout << "key is:\t" << hex << key << "\n";
          colIndex++;
        }
        else{
          size_t size = str.size();
          Value += str;
          Value.at(colIndex) = size;
         // std::cout<<"colIndex: "<<colIndex<<"size: "<<size;
          colIndex++;
        }
        
        
      }
       //std::cout<<"befor put: "<<key<<"\n";
      client.put(key, Value); //读完一行，push 数据。
    }
  }
  inFile.close();
}

void ldbc_BI1(viper::Viper<std::string, std::string>::Client &client)
{
}

int main(int argc, char **argv)
{
  size_t initial_base_size = 1073741824; // 1 GiB
  initial_base_size *= 1;
  const size_t initial_size = initial_base_size; // 1 GiB
                                                 //  auto viper_db = viper::Viper<uint64_t, uint64_t>::create("/mnt/pmem0/test-lmx", initial_size);
  auto viper_db = viper::Viper<std::string, std::string>::create("/mnt/pmem0/test-lmx", initial_size);

  // To modify records in Viper, you need to use a Viper Client.
  auto person_client = viper_db->get_client();
  std::string personFile{"/home/kvgroup/lmx/tag/property-graph/dataset/ldbc_dataset/person.csv"};
  vertex_writer(person_client, personFile, PERSON);

 std::ofstream ofs;
ofs.open("scan-result.txt", std::ios::app);
auto startIndex = std::chrono::steady_clock::now();
 
  int male{0};
  int female{0};
 for(uint32_t i=0;i<9891;i++){
  uint32_t Vid = i;
  Vid |= PERSON;
  uint64_t key = Vid;
  key = key << 32;
  std::string value;
  person_client.get(key, &value);
  size_t start = value.at(0) + value.at(1) +value.at(2);
  size_t length = value.at(3);
  //std::cout <<"gender is: " <<value.substr(start,length)<<"\n";
  std::string gender = value.substr(start,length);
  if(gender == "male"){
    male++;
  }else{
    female++;

  }
 }
  auto endIndex = std::chrono::steady_clock::now();
  std::chrono::duration<double, std::milli> index_ms = endIndex - startIndex;
   ofs << "Index:"
        << "\n";

    ofs << "scanFromIndex:  " << "\t";
    ofs << "time: " << index_ms.count() << "ms"
        << "\n\n";

  cout<<"male is: "<<male<<std::endl;
  cout<<"female is: "<<female<<std::endl;


    auto startData = std::chrono::steady_clock::now();
   person_client.scan_test();
   auto endData = std::chrono::steady_clock::now();
   std::chrono::duration<double, std::milli> data_ms = endData - startData;
       ofs << "scanFromData:  " << "\t";
    ofs << "time: " << data_ms.count() << "ms"
        << "\n\n";

    ofs.close();


  return 0;
}

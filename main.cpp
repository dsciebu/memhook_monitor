#include <cstring>

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <source_location>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

extern "C" {
#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_rma.h>
}

namespace {

fi_info *makeHints() {
  fi_info *hints = fi_allocinfo();
  if (!hints) {
    throw std::runtime_error("Hints allocation failed");
  }

  hints->domain_attr->threading = FI_THREAD_SAFE;
  hints->domain_attr->mr_mode =
      FI_MR_LOCAL | FI_MR_ALLOCATED | FI_MR_PROV_KEY | FI_MR_VIRT_ADDR;
  hints->domain_attr->name = nullptr;
  hints->fabric_attr->prov_name = strdup("verbs");

  hints->domain_attr->resource_mgmt = FI_RM_ENABLED;
  hints->ep_attr->type = FI_EP_RDM;
  hints->ep_attr->protocol = FI_PROTO_RXM;

  hints->addr_format = FI_FORMAT_UNSPEC;
  hints->dest_addr = nullptr;
  hints->mode = FI_CONTEXT;
  hints->domain_attr->control_progress = FI_PROGRESS_AUTO;
  hints->domain_attr->data_progress = FI_PROGRESS_AUTO;
  hints->caps = FI_MSG | FI_RMA | FI_TAGGED | FI_SOURCE | FI_DIRECTED_RECV;
  hints->tx_attr->op_flags = FI_TRANSMIT_COMPLETE;

  return hints;
}

auto getAddr(fid *endpoint) {
  std::size_t addrLen = 0;
  auto ret = fi_getname(endpoint, nullptr, &addrLen);
  if ((ret != -FI_ETOOSMALL) || (addrLen <= 0)) {
    throw std::runtime_error("Unexpected error");
  }

  auto data = std::vector<char>(addrLen, 0);

  ret = fi_getname(endpoint, static_cast<void *>(data.data()), &addrLen);
  if (ret) {
    throw std::runtime_error("Unexpected error");
  }

  data.shrink_to_fit();
  return data;
}

void CHECK(int ret, const std::source_location location =
                        std::source_location::current()) {
  if (ret) {
    std::ostringstream oss;
    oss << "Check failed file: " << location.file_name() << "("
        << location.line() << ":" << location.column() << ") `"
        << location.function_name() << "`: " << fi_strerror(ret);

    throw std::runtime_error(oss.str());
  }
}

} // namespace

int main() {
  static constexpr size_t pageSize{4096};
  static constexpr size_t bufferSize{51 * pageSize};

  const auto domainLam = [&] {
    fi_info *hints = makeHints();
    fi_info *info = nullptr;
    fid_fabric *fabric = nullptr;
    fid_domain *domain = nullptr;

    CHECK(fi_getinfo(fi_version(), nullptr, nullptr, 0, hints, &info));

    CHECK(fi_fabric(info->fabric_attr, &fabric, nullptr));

    for (int i = 0; i < 500; i++) {
      CHECK(fi_domain(fabric, info, &domain, nullptr));
      fi_close(&domain->fid);
    }

    fi_close(&fabric->fid);
    fi_freeinfo(info);
    hints->dest_addr = nullptr;
    fi_freeinfo(hints);
  };

  std::mutex mtx;
  std::condition_variable condVar;
  const auto bufOperationsLam = [&](std::stop_token stoken) {
    while (!stoken.stop_requested()) {
      char *ptr = new char[bufferSize];
      memset(ptr, 321, bufferSize);
      delete[] ptr;
    }
  };

  std::vector<std::jthread> bufThrs(50);

  for (auto &t : bufThrs) {
    t = std::jthread(bufOperationsLam);
  }

  std::vector<std::thread> thrs(50);
  for (auto &t : thrs) {
    t = std::thread(domainLam);
  }
  for (auto &t : thrs) {
    t.join();
  }

  for (auto &t : bufThrs) {
    t.request_stop();
  }
}

package com.antgroup.geaflow.console.web.controller.api;

import com.antgroup.geaflow.console.biz.shared.FunctionManager;
import com.antgroup.geaflow.console.biz.shared.view.FunctionView;
import com.antgroup.geaflow.console.common.dal.model.FunctionSearch;
import com.antgroup.geaflow.console.common.dal.model.PageList;
import com.antgroup.geaflow.console.web.api.GeaflowApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
public class FunctionController {

    @Autowired
    private FunctionManager functionManager;

    @GetMapping("/instances/{instanceName}/functions")
    public GeaflowApiResponse<PageList<FunctionView>> instanceSearch(@PathVariable("instanceName") String instanceName,
                                                                     FunctionSearch search) {
        return GeaflowApiResponse.success(functionManager.searchByInstanceName(instanceName, search));
    }

    @GetMapping("/instances/{instanceName}/functions/{functionName}")
    public GeaflowApiResponse<FunctionView> getFunction(@PathVariable String instanceName,
                                                        @PathVariable String functionName) {
        return GeaflowApiResponse.success(functionManager.getByName(instanceName, functionName));
    }

    @PostMapping("/instances/{instanceName}/functions")
    public GeaflowApiResponse<String> createFunction(@PathVariable String instanceName, FunctionView view,
                                                     @RequestParam(required = false) MultipartFile functionFile,
                                                     @RequestParam(required = false) String fileId) {
        return GeaflowApiResponse.success(functionManager.createFunction(instanceName, view, functionFile, fileId));
    }

    @PutMapping("/instances/{instanceName}/functions/{functionName}")
    public GeaflowApiResponse<Boolean> updateFunction(@PathVariable String instanceName, @PathVariable String functionName,
                                                      FunctionView view, @RequestParam(required = false) MultipartFile functionFile) {
        return GeaflowApiResponse.success(functionManager.updateFunction(instanceName, functionName, view, functionFile));
    }

    @DeleteMapping("/instances/{instanceName}/functions/{functionName}")
    public GeaflowApiResponse<Boolean> deleteFunction(@PathVariable String instanceName,
                                                      @PathVariable String functionName) {
        return GeaflowApiResponse.success(functionManager.deleteFunction(instanceName, functionName));
    }
}

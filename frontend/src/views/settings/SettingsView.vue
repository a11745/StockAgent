<script setup lang="ts">
import { ref } from 'vue'
import { useUserStore } from '@/stores/user'
import { ElMessage } from 'element-plus'
import type { FormInstance, FormRules } from 'element-plus'

const userStore = useUserStore()

// 活跃标签页
const activeTab = ref('profile')

// 个人信息表单
const profileFormRef = ref<FormInstance>()
const profileForm = ref({
  nickname: userStore.nickname,
  email: userStore.userInfo?.email || '',
})
const profileSaving = ref(false)

// 偏好设置
const preferences = ref({
  theme: (userStore.preferences as any)?.theme || 'light',
  notification_enabled: (userStore.preferences as any)?.notification_enabled ?? true,
})
const prefSaving = ref(false)

// 密码修改表单
const passwordFormRef = ref<FormInstance>()
const passwordForm = ref({
  oldPassword: '',
  newPassword: '',
  confirmPassword: '',
})
const passwordSaving = ref(false)

const passwordRules: FormRules = {
  oldPassword: [{ required: true, message: '请输入当前密码', trigger: 'blur' }],
  newPassword: [
    { required: true, message: '请输入新密码', trigger: 'blur' },
    { min: 8, message: '密码长度不能少于 8 个字符', trigger: 'blur' },
  ],
  confirmPassword: [
    { required: true, message: '请确认新密码', trigger: 'blur' },
    {
      validator: (_rule, value, callback) => {
        if (value !== passwordForm.value.newPassword) {
          callback(new Error('两次输入的密码不一致'))
        } else {
          callback()
        }
      },
      trigger: 'blur',
    },
  ],
}

// 保存个人信息
async function saveProfile() {
  profileSaving.value = true
  try {
    // TODO: 实现更新用户信息 API
    ElMessage.success('保存成功')
  } finally {
    profileSaving.value = false
  }
}

// 保存偏好设置
async function savePreferences() {
  prefSaving.value = true
  try {
    await userStore.updatePreferences(preferences.value)
    ElMessage.success('设置已保存')
  } finally {
    prefSaving.value = false
  }
}

// 修改密码
async function changePassword() {
  const valid = await passwordFormRef.value?.validate()
  if (!valid) return
  
  passwordSaving.value = true
  try {
    // 调用修改密码 API
    ElMessage.success('密码修改成功')
    passwordForm.value = { oldPassword: '', newPassword: '', confirmPassword: '' }
  } finally {
    passwordSaving.value = false
  }
}
</script>

<template>
  <div class="settings-view">
    <h1>设置</h1>
    
    <el-tabs v-model="activeTab" class="settings-tabs">
      <!-- 个人信息 -->
      <el-tab-pane label="个人信息" name="profile">
        <div class="settings-section card">
          <el-form
            ref="profileFormRef"
            :model="profileForm"
            label-width="100px"
            size="large"
          >
            <el-form-item label="用户名">
              <el-input :value="userStore.username" disabled />
            </el-form-item>
            
            <el-form-item label="昵称">
              <el-input v-model="profileForm.nickname" placeholder="输入昵称" />
            </el-form-item>
            
            <el-form-item label="邮箱">
              <el-input v-model="profileForm.email" disabled />
            </el-form-item>
            
            <el-form-item>
              <el-button type="primary" :loading="profileSaving" @click="saveProfile">
                保存修改
              </el-button>
            </el-form-item>
          </el-form>
        </div>
      </el-tab-pane>
      
      <!-- 偏好设置 -->
      <el-tab-pane label="偏好设置" name="preferences">
        <div class="settings-section card">
          <el-form label-width="100px" size="large">
            <el-form-item label="主题">
              <el-radio-group v-model="preferences.theme">
                <el-radio value="light">浅色</el-radio>
                <el-radio value="dark">深色</el-radio>
              </el-radio-group>
            </el-form-item>
            
            <el-form-item label="消息通知">
              <el-switch v-model="preferences.notification_enabled" />
            </el-form-item>
            
            <el-form-item>
              <el-button type="primary" :loading="prefSaving" @click="savePreferences">
                保存设置
              </el-button>
            </el-form-item>
          </el-form>
        </div>
      </el-tab-pane>
      
      <!-- 安全设置 -->
      <el-tab-pane label="安全设置" name="security">
        <div class="settings-section card">
          <h3>修改密码</h3>
          
          <el-form
            ref="passwordFormRef"
            :model="passwordForm"
            :rules="passwordRules"
            label-width="100px"
            size="large"
            style="max-width: 400px"
          >
            <el-form-item label="当前密码" prop="oldPassword">
              <el-input
                v-model="passwordForm.oldPassword"
                type="password"
                show-password
              />
            </el-form-item>
            
            <el-form-item label="新密码" prop="newPassword">
              <el-input
                v-model="passwordForm.newPassword"
                type="password"
                show-password
              />
            </el-form-item>
            
            <el-form-item label="确认密码" prop="confirmPassword">
              <el-input
                v-model="passwordForm.confirmPassword"
                type="password"
                show-password
              />
            </el-form-item>
            
            <el-form-item>
              <el-button type="primary" :loading="passwordSaving" @click="changePassword">
                修改密码
              </el-button>
            </el-form-item>
          </el-form>
        </div>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<style lang="scss" scoped>
.settings-view {
  max-width: 800px;
  margin: 0 auto;
  
  h1 {
    font-size: 24px;
    font-weight: 600;
    margin-bottom: 24px;
  }
}

.settings-tabs {
  :deep(.el-tabs__content) {
    padding-top: 20px;
  }
}

.settings-section {
  padding: 24px;
  
  h3 {
    margin-bottom: 20px;
  }
}
</style>

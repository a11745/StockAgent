<script setup lang="ts">
import { ref, reactive } from 'vue'
import { useRouter } from 'vue-router'
import { useAuth } from '@/hooks/useAuth'
import type { FormInstance, FormRules } from 'element-plus'

const router = useRouter()
const { register, isLoading } = useAuth()

// 表单引用
const formRef = ref<FormInstance>()

// 表单数据
const formData = reactive({
  username: '',
  email: '',
  password: '',
  confirmPassword: '',
  nickname: '',
  agree: false,
})

// 验证规则
const rules: FormRules = {
  username: [
    { required: true, message: '请输入用户名', trigger: 'blur' },
    { min: 3, max: 20, message: '用户名长度在 3 到 20 个字符', trigger: 'blur' },
  ],
  email: [
    { required: true, message: '请输入邮箱', trigger: 'blur' },
    { type: 'email', message: '请输入正确的邮箱格式', trigger: 'blur' },
  ],
  password: [
    { required: true, message: '请输入密码', trigger: 'blur' },
    { min: 8, message: '密码长度不能少于 8 个字符', trigger: 'blur' },
  ],
  confirmPassword: [
    { required: true, message: '请确认密码', trigger: 'blur' },
    {
      validator: (_rule, value, callback) => {
        if (value !== formData.password) {
          callback(new Error('两次输入的密码不一致'))
        } else {
          callback()
        }
      },
      trigger: 'blur',
    },
  ],
  agree: [
    {
      validator: (_rule, value, callback) => {
        if (!value) {
          callback(new Error('请阅读并同意服务条款'))
        } else {
          callback()
        }
      },
      trigger: 'change',
    },
  ],
}

// 提交注册
async function handleSubmit() {
  const valid = await formRef.value?.validate()
  if (!valid) return
  
  await register({
    username: formData.username,
    email: formData.email,
    password: formData.password,
    nickname: formData.nickname || undefined,
  })
}
</script>

<template>
  <div class="register-view">
    <h2 class="title">创建账户</h2>
    <p class="subtitle">开始您的智能投资之旅</p>
    
    <el-form
      ref="formRef"
      :model="formData"
      :rules="rules"
      size="large"
      @submit.prevent="handleSubmit"
    >
      <el-form-item prop="username">
        <el-input
          v-model="formData.username"
          placeholder="用户名"
          :prefix-icon="User"
        />
      </el-form-item>
      
      <el-form-item prop="email">
        <el-input
          v-model="formData.email"
          placeholder="邮箱"
          :prefix-icon="Message"
        />
      </el-form-item>
      
      <el-form-item prop="nickname">
        <el-input
          v-model="formData.nickname"
          placeholder="昵称（可选）"
          :prefix-icon="UserFilled"
        />
      </el-form-item>
      
      <el-form-item prop="password">
        <el-input
          v-model="formData.password"
          type="password"
          placeholder="密码（至少8位）"
          :prefix-icon="Lock"
          show-password
        />
      </el-form-item>
      
      <el-form-item prop="confirmPassword">
        <el-input
          v-model="formData.confirmPassword"
          type="password"
          placeholder="确认密码"
          :prefix-icon="Lock"
          show-password
        />
      </el-form-item>
      
      <el-form-item prop="agree">
        <el-checkbox v-model="formData.agree">
          我已阅读并同意
          <el-link type="primary" :underline="false">服务条款</el-link>
          和
          <el-link type="primary" :underline="false">隐私政策</el-link>
        </el-checkbox>
      </el-form-item>
      
      <el-form-item>
        <el-button
          type="primary"
          native-type="submit"
          :loading="isLoading"
          class="submit-btn"
        >
          注册
        </el-button>
      </el-form-item>
    </el-form>
    
    <div class="login-link">
      已有账户?
      <el-link type="primary" @click="router.push('/login')">
        立即登录
      </el-link>
    </div>
  </div>
</template>

<script lang="ts">
import { User, UserFilled, Lock, Message } from '@element-plus/icons-vue'
export default {
  data() {
    return { User, UserFilled, Lock, Message }
  }
}
</script>

<style lang="scss" scoped>
.register-view {
  .title {
    font-size: 28px;
    font-weight: 700;
    color: var(--text-primary);
    margin-bottom: 8px;
  }
  
  .subtitle {
    color: var(--text-secondary);
    margin-bottom: 32px;
  }
  
  .el-form {
    .el-form-item {
      margin-bottom: 18px;
    }
    
    :deep(.el-input__wrapper) {
      border-radius: 10px;
      padding: 4px 12px;
    }
  }
  
  .submit-btn {
    width: 100%;
    height: 48px;
    border-radius: 10px;
    font-size: 16px;
  }
  
  .login-link {
    text-align: center;
    margin-top: 24px;
    color: var(--text-secondary);
  }
}
</style>
